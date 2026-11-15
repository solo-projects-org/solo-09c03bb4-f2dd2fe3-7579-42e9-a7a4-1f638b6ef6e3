"""有租期的双端交接：领取、按端摘要回报、下载。

语义要点：

- 领取仅对 ``ready`` 批开放；一次领取对 R1、R2 原档分别计算 SHA-256，
  并把交接号、两端摘要与大小、到期时间在**同一个事务**里写入，因此领取失败
  （文件缺失、读盘错误、摘要计算中断）不会留下半份交接记录。
- 同一批在租期内重复/并发领取只会得到同一个交接号；租期过后可以重新领取，
  旧交接被标记为 ``expired``，其迟到确认会被拒绝，绝不会影响新交接。
- worker 按端回报收到的文件摘要，只有两端都吻合才把交接置为 ``completed``。
- 交接记录与确认记录都持久化：重启后 ``completed`` 保留，``active`` 沿用原到期时间。
"""

from __future__ import annotations

import asyncio
import hashlib
import threading
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Mapping

from fastapi import HTTPException

from .database import get_conn
from .models import BatchStatus, FileRole, HandoffStatus, iso_after, iso_seconds_remaining, utcnow_iso

if TYPE_CHECKING:  # pragma: no cover - 仅用于类型提示，避免运行期循环导入
    from .service import BatchService

_CHUNK = 1024 * 1024
_DIGEST_HEX_LENGTH = 64


class HandoffService:
    def __init__(self, settings, batch_service: "BatchService") -> None:
        self.settings = settings
        self.batches = batch_service
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    # ------------------------------------------------------------------ #
    # 小工具
    # ------------------------------------------------------------------ #
    @property
    def lease_seconds(self) -> float:
        return float(self.settings.handoff_lease_seconds)

    def _batch_row(self, batch_id: str) -> dict | None:
        return self.batches._row(batch_id)

    def _path(self, batch_id: str, role: FileRole) -> Path:
        return self.batches._fastq_path(batch_id, role)

    def _lock_for(self, batch_id: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._locks.get(batch_id)
            if lock is None:
                lock = threading.Lock()
                self._locks[batch_id] = lock
            return lock

    @staticmethod
    def _now() -> str:
        return utcnow_iso()

    @staticmethod
    def _effective_status(row: Mapping, now: str) -> str:
        if row["status"] == HandoffStatus.ACTIVE.value and row["expires_at"] <= now:
            return HandoffStatus.EXPIRED.value
        return row["status"]

    def _active_row(self, batch_id: str) -> dict | None:
        with get_conn(self.settings.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM handoffs WHERE batch_id = ? AND status = ?"
                " ORDER BY created_at DESC LIMIT 1",
                (batch_id, HandoffStatus.ACTIVE.value),
            ).fetchone()
            return dict(row) if row else None

    def _handoff_row(self, handoff_id: str) -> dict | None:
        with get_conn(self.settings.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM handoffs WHERE id = ?", (handoff_id,)
            ).fetchone()
            return dict(row) if row else None

    def _confirmations(self, handoff_id: str) -> dict[str, bool]:
        with get_conn(self.settings.db_path) as conn:
            rows = conn.execute(
                "SELECT role, matched FROM handoff_confirmations WHERE handoff_id = ?",
                (handoff_id,),
            ).fetchall()
        return {r["role"]: bool(r["matched"]) for r in rows}

    def _download_path(self, handoff_id: str, role: FileRole) -> str:
        return f"/handoffs/{handoff_id}/files/{role.value}/download"

    def _response(
        self, row: Mapping, confirmations: Mapping[str, bool], *, reused: bool, message: str
    ) -> dict:
        now = self._now()
        status = self._effective_status(row, now)
        download_urls = {
            role.value: self._download_path(row["id"], role) for role in FileRole
        }
        ends = {}
        for role in FileRole:
            ends[role.value.lower()] = {
                "role": role.value,
                "sha256": row[f"{role.value.lower()}_sha256"],
                "size": row[f"{role.value.lower()}_size"],
                "download_url": download_urls[role.value],
                "download_path": download_urls[role.value],
                "confirmed": bool(confirmations.get(role.value, False)),
            }
        return {
            "handoff_id": row["id"],
            "batch_id": row["batch_id"],
            "sample_id": row["sample_id"],
            "status": status,
            "lease_seconds": int(row["lease_seconds"]),
            "expires_at": row["expires_at"],
            "seconds_remaining": iso_seconds_remaining(row["expires_at"])
            if status == HandoffStatus.ACTIVE.value
            else 0,
            "r1": ends["r1"],
            "r2": ends["r2"],
            "downloads": download_urls,
            "completed_at": row["completed_at"],
            "reused": reused,
            "message": message,
        }

    # ------------------------------------------------------------------ #
    # 领取
    # ------------------------------------------------------------------ #
    async def claim(self, batch_id: str) -> dict:
        row = self._batch_row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] != BatchStatus.READY.value:
            raise HTTPException(
                status_code=409,
                detail=f"batch is {row['status']}, handoff requires ready",
            )

        # 租期内已存在交接：同一批并发/重复领取必须拿到同一个交接号，不再重复计算摘要
        active = self._active_row(batch_id)
        if active is not None:
            now = self._now()
            if active["expires_at"] > now:
                active["sample_id"] = row["sample_id"]
                return self._response(
                    active, self._confirmations(active["id"]),
                    reused=True,
                    message="reused active handoff within lease",
                )

        paths = {role: self._path(batch_id, role) for role in FileRole}
        for role, path in paths.items():
            if not path.exists():
                raise HTTPException(
                    status_code=409,
                    detail=f"{role.value} file missing on disk, cannot hand off",
                )

        # 摘要计算在写库之前完成：读盘失败不会留下任何交接记录
        try:
            digests = await asyncio.to_thread(_hash_pair, paths[FileRole.R1], paths[FileRole.R2])
        except OSError as exc:
            raise HTTPException(
                status_code=500, detail=f"failed to compute SHA-256 for handoff: {exc}"
            ) from exc

        # 摘要计算期间批次可能被取消/删除，重查后再落库
        row = self._batch_row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] != BatchStatus.READY.value:
            raise HTTPException(
                status_code=409,
                detail=f"batch is {row['status']}, handoff requires ready",
            )

        claimed = await asyncio.to_thread(
            self._claim_sync, batch_id, digests[FileRole.R1], digests[FileRole.R2]
        )
        claimed_row, reused = claimed
        claimed_row["sample_id"] = self._sample_id(batch_id)
        message = (
            "reused active handoff within lease"
            if reused
            else "handoff created; both ends must report matching sha256"
        )
        return self._response(
            claimed_row, self._confirmations(claimed_row["id"]),
            reused=reused, message=message,
        )

    def _claim_sync(
        self,
        batch_id: str,
        r1: tuple[str, int],
        r2: tuple[str, int],
    ) -> tuple[dict, bool]:
        """临界区：重查批次与已有交接，必要时把过期交接置 expired 并原子插入新交接。"""
        with self._lock_for(batch_id), get_conn(self.settings.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="batch not found")
            if row["status"] != BatchStatus.READY.value:
                raise HTTPException(
                    status_code=409,
                    detail=f"batch is {row['status']}, handoff requires ready",
                )

            now = self._now()
            active = conn.execute(
                "SELECT * FROM handoffs WHERE batch_id = ? AND status = ?"
                " ORDER BY created_at DESC LIMIT 1",
                (batch_id, HandoffStatus.ACTIVE.value),
            ).fetchone()
            if active is not None:
                active = dict(active)
                if active["expires_at"] > now:
                    # 另一请求（或另一进程）已抢先领取，返回它的交接号
                    active["sample_id"] = row["sample_id"]
                    return active, True
                # 租期已过：旧交接作废；迟到确认随后会被拒绝
                conn.execute(
                    "UPDATE handoffs SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
                    (HandoffStatus.EXPIRED.value, now, active["id"], HandoffStatus.ACTIVE.value),
                )

            handoff_id = uuid.uuid4().hex
            expires_at = iso_after(self.lease_seconds)
            conn.execute(
                "INSERT INTO handoffs (id, batch_id, status, lease_seconds, expires_at,"
                " created_at, updated_at, completed_at, r1_sha256, r2_sha256, r1_size, r2_size)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)",
                (
                    handoff_id,
                    batch_id,
                    HandoffStatus.ACTIVE.value,
                    self.lease_seconds,
                    expires_at,
                    now,
                    now,
                    r1[0],
                    r2[0],
                    r1[1],
                    r2[1],
                ),
            )
            inserted = conn.execute(
                "SELECT * FROM handoffs WHERE id = ?", (handoff_id,)
            ).fetchone()
            result = dict(inserted)
            result["sample_id"] = row["sample_id"]
            return result, False

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    async def get(self, handoff_id: str, batch_id: str | None = None) -> dict:
        row = self._handoff_row(handoff_id)
        if row is None or (batch_id is not None and row["batch_id"] != batch_id):
            raise HTTPException(status_code=404, detail="handoff not found")
        now = self._now()
        if row["status"] == HandoffStatus.ACTIVE.value and row["expires_at"] <= now:
            self._mark_expired(handoff_id)
            row["status"] = HandoffStatus.EXPIRED.value
        row["sample_id"] = self._sample_id(row["batch_id"])
        return self._response(
            row, self._confirmations(handoff_id), reused=False, message="handoff detail"
        )

    def _sample_id(self, batch_id: str) -> str:
        row = self._batch_row(batch_id)
        return row["sample_id"] if row else ""

    def _mark_expired(self, handoff_id: str) -> None:
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE handoffs SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
                (HandoffStatus.EXPIRED.value, self._now(), handoff_id, HandoffStatus.ACTIVE.value),
            )

    # ------------------------------------------------------------------ #
    # 按端回报摘要
    # ------------------------------------------------------------------ #
    async def confirm(self, handoff_id: str, role: FileRole, sha256: str) -> dict:
        digest = sha256.strip().lower()
        if len(digest) != _DIGEST_HEX_LENGTH or any(c not in "0123456789abcdef" for c in digest):
            raise HTTPException(
                status_code=422,
                detail="sha256 must be a 64-character hexadecimal digest",
            )
        return await asyncio.to_thread(self._confirm_sync, handoff_id, role, digest)

    def _confirm_sync(self, handoff_id: str, role: FileRole, digest: str) -> dict:
        # 租期判定放在写事务之外：这样「置 expired」能独立提交，不会被随后的 409 回滚
        pre = self._handoff_row(handoff_id)
        if pre is None:
            raise HTTPException(status_code=404, detail="handoff not found")
        if (
            pre["status"] == HandoffStatus.ACTIVE.value
            and pre["expires_at"] <= self._now()
        ):
            # 到期（且未被重领）的交接就此作废；旧号的任何迟到确认都不得生效
            self._mark_expired(handoff_id)

        with get_conn(self.settings.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM handoffs WHERE id = ?", (handoff_id,)
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="handoff not found")
            row = dict(row)
            now = self._now()
            batch = conn.execute(
                "SELECT status, sample_id FROM batches WHERE id = ?", (row["batch_id"],)
            ).fetchone()
            sample_id = batch["sample_id"] if batch else ""
            batch_status = batch["status"] if batch else None

            expected = row[f"{role.value.lower()}_sha256"]
            matched = digest == expected
            confirmations = {
                r["role"]: bool(r["matched"])
                for r in conn.execute(
                    "SELECT role, matched FROM handoff_confirmations WHERE handoff_id = ?",
                    (handoff_id,),
                ).fetchall()
            }

            # 已完成：同样的摘要重复回报按幂等成功返回，但绝不改状态
            if row["status"] == HandoffStatus.COMPLETED.value:
                if matched:
                    return self._confirm_response(
                        row, role, confirmations, matched=True,
                        status=HandoffStatus.COMPLETED.value,
                        message="handoff already completed (idempotent)",
                    )
                raise HTTPException(
                    status_code=409, detail="handoff already completed, cannot change"
                )

            # 已过期 / 已被新交接取代 / 批次不再是 ready：迟到确认一律拒绝，不做任何写入
            if row["status"] != HandoffStatus.ACTIVE.value:
                raise HTTPException(
                    status_code=409,
                    detail=f"handoff is {row['status']}, confirmation rejected",
                )
            if row["expires_at"] <= now:
                raise HTTPException(
                    status_code=409,
                    detail="handoff is expired, confirmation rejected",
                )
            if batch_status != BatchStatus.READY.value:
                raise HTTPException(
                    status_code=409,
                    detail=f"batch is {batch_status}, handoff no longer deliverable",
                )

            # 每端只保留一条已吻合的回报：先到的不吻合记录可被后到的吻合记录升级，
            # 已吻合的记录不会被后来的不吻合回报降级。
            previous = conn.execute(
                "SELECT matched, received_sha256 FROM handoff_confirmations"
                " WHERE handoff_id = ? AND role = ?",
                (handoff_id, role.value),
            ).fetchone()
            if previous is not None and previous["matched"] and not matched:
                matched = True  # 保留已吻合结论，忽略这次异常回报
                digest = previous["received_sha256"]
            if previous is None or (matched and not previous["matched"]):
                conn.execute(
                    "INSERT INTO handoff_confirmations"
                    " (handoff_id, role, received_sha256, matched, confirmed_at)"
                    " VALUES (?, ?, ?, ?, ?)"
                    " ON CONFLICT(handoff_id, role) DO UPDATE SET"
                    " received_sha256 = excluded.received_sha256,"
                    " matched = excluded.matched, confirmed_at = excluded.confirmed_at",
                    (handoff_id, role.value, digest, 1 if matched else 0, now),
                )
            confirmations[role.value] = matched

            both = all(confirmations.get(r.value, False) for r in FileRole)
            status = HandoffStatus.ACTIVE.value
            completed_at = row["completed_at"]
            if both:
                conn.execute(
                    "UPDATE handoffs SET status = ?, completed_at = ?, updated_at = ?"
                    " WHERE id = ? AND status = ?",
                    (
                        HandoffStatus.COMPLETED.value,
                        now,
                        now,
                        handoff_id,
                        HandoffStatus.ACTIVE.value,
                    ),
                )
                status = HandoffStatus.COMPLETED.value
                completed_at = now

            row["status"] = status
            row["completed_at"] = completed_at
            row["sample_id"] = sample_id
            return self._confirm_response(
                row, role, confirmations, matched=matched, status=status,
                message=(
                    "both ends confirmed, handoff completed"
                    if both
                    else "digest matched; waiting for the other end"
                    if matched
                    else "digest mismatch, handoff remains active"
                ),
            )

    def _confirm_response(
        self,
        row: Mapping,
        role: FileRole,
        confirmations: Mapping[str, bool],
        *,
        matched: bool,
        status: str,
        message: str,
    ) -> dict:
        return {
            "handoff_id": row["id"],
            "batch_id": row["batch_id"],
            "role": role,
            "matched": matched,
            "status": status,
            "completed": status == HandoffStatus.COMPLETED.value,
            "r1_confirmed": bool(confirmations.get(FileRole.R1.value, False)),
            "r2_confirmed": bool(confirmations.get(FileRole.R2.value, False)),
            "expires_at": row["expires_at"],
            "completed_at": row["completed_at"],
            "message": message,
        }

    # ------------------------------------------------------------------ #
    # 交接下载
    # ------------------------------------------------------------------ #
    def download_path(
        self, handoff_id: str, role: FileRole, batch_id: str | None = None
    ) -> tuple[Path, str]:
        row = self._handoff_row(handoff_id)
        if row is None or (batch_id is not None and row["batch_id"] != batch_id):
            raise HTTPException(status_code=404, detail="handoff not found")
        now = self._now()
        status = self._effective_status(row, now)
        if status == HandoffStatus.EXPIRED.value:
            raise HTTPException(
                status_code=409, detail="handoff expired, claim a new handoff"
            )
        batch = self._batch_row(row["batch_id"])
        if batch is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if batch["status"] != BatchStatus.READY.value:
            raise HTTPException(
                status_code=409,
                detail=f"batch is {batch['status']}, not downloadable",
            )
        path = self._path(row["batch_id"], role)
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"{role.value} file missing on disk")
        return path, f"{batch['sample_id']}_{role.value}.fastq"

    # ------------------------------------------------------------------ #
    # 重启恢复
    # ------------------------------------------------------------------ #
    def recover(self) -> dict:
        """保留 completed；active 沿用原到期时间；批次已不可交付的 active 置 expired。"""
        expired = 0
        kept_active = 0
        completed = 0
        with get_conn(self.settings.db_path) as conn:
            rows = conn.execute(
                "SELECT h.id, h.status, h.expires_at, b.status AS batch_status"
                " FROM handoffs h JOIN batches b ON b.id = h.batch_id"
                " WHERE h.status = ?",
                (HandoffStatus.ACTIVE.value,),
            ).fetchall()
            for row in rows:
                if row["batch_status"] != BatchStatus.READY.value:
                    conn.execute(
                        "UPDATE handoffs SET status = ?, updated_at = ?"
                        " WHERE id = ? AND status = ?",
                        (
                            HandoffStatus.EXPIRED.value,
                            self._now(),
                            row["id"],
                            HandoffStatus.ACTIVE.value,
                        ),
                    )
                    expired += 1
                else:
                    kept_active += 1
            completed = conn.execute(
                "SELECT COUNT(*) FROM handoffs WHERE status = ?",
                (HandoffStatus.COMPLETED.value,),
            ).fetchone()[0]
        return {
            "active_preserved": kept_active,
            "expired": expired,
            "completed_preserved": completed,
        }


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(_CHUNK)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _hash_pair(r1_path: Path, r2_path: Path) -> dict[FileRole, tuple[str, int]]:
    return {FileRole.R1: _hash_file(r1_path), FileRole.R2: _hash_file(r2_path)}
