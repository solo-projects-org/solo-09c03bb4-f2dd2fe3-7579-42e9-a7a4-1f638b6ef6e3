"""批次生命周期服务：创建、上传校验、取消、回收、重启恢复。"""

from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

from fastapi import HTTPException

from .database import get_conn, init_db
from .models import BatchStatus, FileRole, utcnow_iso
from .storage import (
    BatchRegistry,
    QuotaExceededError,
    SpaceManager,
    directory_size,
    remove_batch_files,
)
from .validation import StreamError, stream_gunzip, validate_fastq_pair, validate_fastq_structure

TERMINAL_STATUSES = {BatchStatus.READY.value, BatchStatus.FAILED.value, BatchStatus.CANCELLED.value}


class BatchService:
    def __init__(self, settings) -> None:
        self.settings = settings
        init_db(settings.db_path)
        self.space = SpaceManager(settings.batch_byte_limit)
        self.registry = BatchRegistry()
        self._recovered = False

    # ------------------------------------------------------------------ #
    # 路径
    # ------------------------------------------------------------------ #
    def _dir(self, batch_id: str) -> Path:
        return self.settings.batch_dir / batch_id

    def _gz_path(self, batch_id: str, role: FileRole) -> Path:
        return self._dir(batch_id) / f"{role.value}.fastq.gz.uploading"

    def _fastq_path(self, batch_id: str, role: FileRole) -> Path:
        return self._dir(batch_id) / f"{role.value}.fastq"

    # ------------------------------------------------------------------ #
    # DB 小工具
    # ------------------------------------------------------------------ #
    def _row(self, batch_id: str) -> dict | None:
        with get_conn(self.settings.db_path) as conn:
            row = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
            return dict(row) if row else None

    def _files(self, batch_id: str) -> list[dict]:
        with get_conn(self.settings.db_path) as conn:
            rows = conn.execute(
                "SELECT role, size, uploaded_at FROM files WHERE batch_id = ? ORDER BY role",
                (batch_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------ #
    # 创建 / 查询
    # ------------------------------------------------------------------ #
    async def create_batch(self, sample_id: str) -> dict:
        batch_id = uuid.uuid4().hex
        now = utcnow_iso()
        self._dir(batch_id).mkdir(parents=True, exist_ok=True)
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "INSERT INTO batches (id, sample_id, status, byte_limit, error, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, NULL, ?, ?)",
                (batch_id, sample_id, BatchStatus.RECEIVING.value, self.settings.batch_byte_limit, now, now),
            )
        await self.registry.create(batch_id)
        return await self.get_batch_detail(batch_id)

    async def get_batch_detail(self, batch_id: str) -> dict:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        row["files"] = self._files(batch_id)
        row["bytes_used"] = directory_size(self._dir(batch_id))
        return row

    async def list_batches(self) -> list[dict]:
        with get_conn(self.settings.db_path) as conn:
            rows = conn.execute("SELECT id FROM batches ORDER BY created_at").fetchall()
        return [await self.get_batch_detail(r["id"]) for r in rows]

    # ------------------------------------------------------------------ #
    # 终态处理
    # ------------------------------------------------------------------ #
    def _mark_failed(self, batch_id: str, reason: str) -> None:
        now = utcnow_iso()
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE batches SET status = ?, error = ?, updated_at = ? WHERE id = ?",
                (BatchStatus.FAILED.value, reason[:2000], now, batch_id),
            )
            conn.execute("DELETE FROM files WHERE batch_id = ?", (batch_id,))

    async def _cleanup_files_only(self, batch_id: str) -> int:
        """删除本批全部文件并按磁盘真实占用回收配额，但不改批次状态。"""
        freed = await asyncio.to_thread(remove_batch_files, self._dir(batch_id))
        await self.space.release(batch_id, freed)
        await self.space.set_batch_usage(batch_id, directory_size(self._dir(batch_id)))
        return freed

    async def fail_and_cleanup(self, batch_id: str, reason: str) -> int:
        """标记失败，删除本批全部文件并按磁盘真实占用回收配额。"""
        await self._cleanup_files_only(batch_id)
        self._mark_failed(batch_id, reason)
        return 0

    # ------------------------------------------------------------------ #
    # 上传
    # ------------------------------------------------------------------ #
    async def upload(self, batch_id: str, role: FileRole, request) -> dict:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] != BatchStatus.RECEIVING.value:
            raise HTTPException(status_code=409, detail=f"batch is {row['status']}, upload rejected")

        with get_conn(self.settings.db_path) as conn:
            dup = conn.execute(
                "SELECT 1 FROM files WHERE batch_id = ? AND role = ?", (batch_id, role.value)
            ).fetchone()
        if dup:
            raise HTTPException(status_code=409, detail=f"{role.value} already uploaded")

        runtime = await self.registry.create(batch_id)
        if runtime.cancel_event.is_set():
            raise HTTPException(status_code=409, detail="batch is cancelling")

        current_task = asyncio.current_task()
        if current_task is not None:
            runtime.uploads[role] = current_task

        gz_path = self._gz_path(batch_id, role)
        out_path = self._fastq_path(batch_id, role)
        gz_bytes = 0
        try:
            try:
                out_bytes = await stream_gunzip(
                    request=request,
                    gz_path=gz_path,
                    out_path=out_path,
                    space=self.space,
                    batch_id=batch_id,
                    cancel_event=runtime.cancel_event,
                    idle_timeout=self.settings.upload_idle_timeout,
                )
                gz_bytes = gz_path.stat().st_size

                # 单端四行制结构校验（线程内做，避免阻塞事件循环）
                await asyncio.to_thread(validate_fastq_structure, out_path)

                # 压缩临时文件已完成使命，立刻删除并回收其占用配额
                gz_path.unlink()
                await self.space.release(batch_id, gz_bytes)

                now = utcnow_iso()
                with get_conn(self.settings.db_path) as conn:
                    conn.execute(
                        "INSERT INTO files (batch_id, role, path, size, uploaded_at)"
                        " VALUES (?, ?, ?, ?, ?)",
                        (batch_id, role.value, str(out_path), out_bytes, now),
                    )
                    conn.execute(
                        "UPDATE batches SET updated_at = ? WHERE id = ?", (now, batch_id)
                    )
            except QuotaExceededError as exc:
                await self.fail_and_cleanup(batch_id, str(exc))
                raise HTTPException(status_code=413, detail=str(exc)) from exc
            except (StreamError, ValueError, OSError) as exc:
                if runtime.cancel_event.is_set():
                    # 取消导致的中断：只清理磁盘，终态由 cancel() 落库，避免抢占为 failed
                    await self._cleanup_files_only(batch_id)
                    raise HTTPException(status_code=409, detail="batch cancelled") from exc
                await self.fail_and_cleanup(batch_id, f"{role.value} rejected: {exc}")
                raise HTTPException(
                    status_code=422, detail=f"{role.value} rejected: {exc}"
                ) from exc
            except HTTPException:
                raise
            except Exception as exc:  # 客户端断连等兜底
                if runtime.cancel_event.is_set():
                    await self._cleanup_files_only(batch_id)
                    raise HTTPException(status_code=409, detail="batch cancelled") from exc
                await self.fail_and_cleanup(batch_id, f"{role.value} upload failed: {exc}")
                raise HTTPException(
                    status_code=500, detail=f"{role.value} upload failed: {exc}"
                ) from exc

            # 两端都到齐则做配对校验
            detail = await self.get_batch_detail(batch_id)
            roles = {f["role"] for f in detail["files"]}
            if {FileRole.R1, FileRole.R2}.issubset(roles):
                try:
                    await asyncio.to_thread(
                        validate_fastq_pair,
                        self._fastq_path(batch_id, FileRole.R1),
                        self._fastq_path(batch_id, FileRole.R2),
                    )
                except ValueError as exc:
                    await self.fail_and_cleanup(batch_id, f"pair validation failed: {exc}")
                    raise HTTPException(
                        status_code=422, detail=f"pair validation failed: {exc}"
                    ) from exc
                now = utcnow_iso()
                with get_conn(self.settings.db_path) as conn:
                    conn.execute(
                        "UPDATE batches SET status = ?, error = NULL, updated_at = ? WHERE id = ?",
                        (BatchStatus.READY.value, now, batch_id),
                    )
            return await self.get_batch_detail(batch_id)
        finally:
            runtime.uploads.pop(role, None)

    # ------------------------------------------------------------------ #
    # 取消
    # ------------------------------------------------------------------ #
    async def cancel(self, batch_id: str) -> dict:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] in TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail=f"batch already {row['status']}")

        runtime = await self.registry.create(batch_id)
        runtime.cancel_event.set()

        # 等待本批正在进行的写入停止（上传任务退出后不会再落盘）
        if runtime.uploads:
            await asyncio.gather(*runtime.uploads.values(), return_exceptions=True)

        # 上传任务可能在取消信号到达前恰好完成（ready）或自行失败，重新确认终态
        refreshed = self._row(batch_id)
        if refreshed is not None and refreshed["status"] in TERMINAL_STATUSES:
            raise HTTPException(
                status_code=409,
                detail=f"batch already {refreshed['status']} before cancel took effect",
            )

        # 文件删除、空间释放、状态落库全部完成后才确认
        freed = await asyncio.to_thread(remove_batch_files, self._dir(batch_id))
        await self.space.release(batch_id, freed)
        await self.space.set_batch_usage(batch_id, directory_size(self._dir(batch_id)))
        now = utcnow_iso()
        with get_conn(self.settings.db_path) as conn:
            conn.execute(
                "UPDATE batches SET status = ?, error = ?, updated_at = ? WHERE id = ?",
                (BatchStatus.CANCELLED.value, "cancelled by operator", now, batch_id),
            )
            conn.execute("DELETE FROM files WHERE batch_id = ?", (batch_id,))
        await self.registry.remove(batch_id)
        return {
            "id": batch_id,
            "status": BatchStatus.CANCELLED,
            "freed_bytes": freed,
            "message": "all writes stopped, files deleted and space released",
        }

    # ------------------------------------------------------------------ #
    # 下载
    # ------------------------------------------------------------------ #
    def download_path(self, batch_id: str, role: FileRole) -> tuple[Path, str]:
        row = self._row(batch_id)
        if row is None:
            raise HTTPException(status_code=404, detail="batch not found")
        if row["status"] != BatchStatus.READY.value:
            raise HTTPException(status_code=409, detail=f"batch is {row['status']}, not downloadable")
        path = self._fastq_path(batch_id, role)
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"{role.value} file missing on disk")
        return path, f"{row['sample_id']}_{role.value}.fastq"

    # ------------------------------------------------------------------ #
    # 重启恢复
    # ------------------------------------------------------------------ #
    async def recover_on_startup(self) -> dict:
        """通过批保留；未完成批（receiving）一律判失败并回收磁盘后再对外服务。"""
        # 同一 service 实例只恢复一次，避免测试中多个 ASGI 客户端重跑 lifespan 误判在途批
        if self._recovered:
            return {"ready": 0, "aborted": 0, "orphaned": 0, "skipped": "already-recovered"}
        self._recovered = True
        self.settings.batch_dir.mkdir(parents=True, exist_ok=True)
        recovered = {"ready": 0, "aborted": 0, "orphaned": 0}

        with get_conn(self.settings.db_path) as conn:
            rows = conn.execute("SELECT id, status FROM batches").fetchall()
            known = {r["id"]: r["status"] for r in rows}

        for batch_id, status in known.items():
            bdir = self._dir(batch_id)
            if status == BatchStatus.READY.value:
                # 以磁盘真实文件为准重建配额账
                await self.space.set_batch_usage(batch_id, directory_size(bdir))
                recovered["ready"] += 1
                continue
            if status == BatchStatus.RECEIVING.value:
                await asyncio.to_thread(remove_batch_files, bdir)
                await self.space.remove_batch(batch_id)
                self._mark_failed(batch_id, "interrupted by server restart")
                recovered["aborted"] += 1
            # failed / cancelled：清理可能残留的文件
            else:
                await asyncio.to_thread(remove_batch_files, bdir)
                await self.space.remove_batch(batch_id)

        # 磁盘上有目录但 DB 无记录的孤儿数据，直接回收
        for child in self.settings.batch_dir.iterdir():
            if child.is_dir() and child.name not in known:
                await asyncio.to_thread(remove_batch_files, child)
                recovered["orphaned"] += 1
        return recovered
