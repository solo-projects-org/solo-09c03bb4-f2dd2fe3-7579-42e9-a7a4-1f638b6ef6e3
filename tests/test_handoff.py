"""有租期的交接 API 测试：摘要、租期、并发、旧号迟到确认、重启恢复。"""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest

from app.config import Settings
from app.database import get_conn
from app.models import HandoffStatus
from app.service import BatchService
from tests.helpers import create_batch as _create_batch
from tests.helpers import good_pair
from tests.helpers import gz_bytes as _gz
from tests.helpers import upload_bytes as _upload_bytes


def _ready_batch(client, sample_id: str = "handoff-sample") -> str:
    r1, r2 = good_pair()
    bid = _create_batch(client, sample_id)
    assert _upload_bytes(client, bid, "R1", _gz(r1)).status_code == 200
    assert _upload_bytes(client, bid, "R2", _gz(r2)).status_code == 200
    assert client.get(f"/batches/{bid}").json()["status"] == "ready"
    return bid


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _claim(client, bid: str):
    return client.post(f"/batches/{bid}/handoffs")


def _confirm(client, handoff_id: str, role: str, sha: str):
    return client.post(f"/handoffs/{handoff_id}/confirm/{role}", json={"sha256": sha})


def _expire_handoff(handoff_id: str, settings) -> None:
    """把交接的到期时间改到过去，模拟租期已过。"""
    from app.models import utcnow_iso

    with get_conn(settings.db_path) as conn:
        conn.execute(
            "UPDATE handoffs SET expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00.000000Z", handoff_id),
        )


def test_claim_returns_digests_and_download_entries(client, settings):
    bid = _ready_batch(client)
    resp = _claim(client, bid)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    hid = body["handoff_id"]
    assert body["batch_id"] == bid
    assert body["status"] == "active"
    assert body["reused"] is False
    assert body["lease_seconds"] == settings.handoff_lease_seconds
    assert body["seconds_remaining"] > 0

    r1_path = settings.batch_dir / bid / "R1.fastq"
    r2_path = settings.batch_dir / bid / "R2.fastq"
    assert body["r1"]["sha256"] == _sha(r1_path)
    assert body["r2"]["sha256"] == _sha(r2_path)
    assert body["r1"]["size"] == r1_path.stat().st_size
    assert body["r2"]["size"] == r2_path.stat().st_size
    assert body["r1"]["download_path"] == f"/handoffs/{hid}/files/R1/download"
    assert body["r1"]["confirmed"] is False and body["r2"]["confirmed"] is False

    # 交接下载入口可取到原档
    dl = client.get(body["r1"]["download_path"])
    assert dl.status_code == 200
    assert dl.content == r1_path.read_bytes()

    # 原 ready 状态与旧下载接口保持不变
    assert client.get(f"/batches/{bid}").json()["status"] == "ready"
    assert client.get(f"/batches/{bid}/files/R1/download").content == r1_path.read_bytes()


def test_claim_requires_ready_and_leaves_no_record(client, settings):
    r1, _ = good_pair()
    bid = _create_batch(client, "handoff-not-ready")
    _upload_bytes(client, bid, "R1", _gz(r1))
    resp = _claim(client, bid)
    assert resp.status_code == 409
    with get_conn(settings.db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM handoffs WHERE batch_id = ?", (bid,)
        ).fetchone()[0] == 0

    assert _claim(client, "does-not-exist").status_code == 404


def test_concurrent_claims_return_same_handoff(client):
    bid = _ready_batch(client, "handoff-concurrent")
    first = _claim(client, bid)
    second = _claim(client, bid)
    assert first.status_code == second.status_code == 201
    a, b = first.json(), second.json()
    assert a["handoff_id"] == b["handoff_id"]
    assert a["r1"]["sha256"] == b["r1"]["sha256"]
    assert a["r2"]["sha256"] == b["r2"]["sha256"]
    assert b["reused"] is True


def test_concurrent_claims_across_threads_single_handoff(client):
    bid = _ready_batch(client, "handoff-threads")
    results = []

    def run():
        results.append(_claim(client, bid))

    import threading

    threads = [threading.Thread(target=run) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ids = {r.json()["handoff_id"] for r in results}
    assert all(r.status_code == 201 for r in results)
    assert len(ids) == 1


def test_completes_only_when_both_ends_match(client, settings):
    bid = _ready_batch(client, "handoff-complete")
    body = _claim(client, bid).json()
    hid = body["handoff_id"]

    only_r1 = _confirm(client, hid, "R1", body["r1"]["sha256"])
    assert only_r1.status_code == 200
    payload = only_r1.json()
    assert payload["matched"] is True
    assert payload["completed"] is False
    assert payload["status"] == "active"
    assert payload["r1_confirmed"] is True and payload["r2_confirmed"] is False
    # 只有 R1 确认时批次仍保持 ready（未整批交付不算完成）
    assert client.get(f"/batches/{bid}").json()["status"] == "ready"

    both = _confirm(client, hid, "R2", body["r2"]["sha256"])
    assert both.status_code == 200
    payload = both.json()
    assert payload["matched"] is True and payload["completed"] is True
    assert payload["status"] == "completed"
    assert payload["completed_at"]

    detail = client.get(f"/handoffs/{hid}").json()
    assert detail["status"] == "completed"
    assert detail["r1"]["confirmed"] and detail["r2"]["confirmed"]


def test_mismatched_digest_does_not_complete(client):
    bid = _ready_batch(client, "handoff-mismatch")
    body = _claim(client, bid).json()
    hid = body["handoff_id"]
    wrong = "0" * 64
    resp = _confirm(client, hid, "R1", wrong)
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["matched"] is False
    assert payload["completed"] is False
    assert payload["r1_confirmed"] is False

    # R1 用正确摘要补报后仍只算单端，R2 不吻合则整批不算完成
    assert _confirm(client, hid, "R1", body["r1"]["sha256"]).json()["r1_confirmed"] is True
    bad_r2 = _confirm(client, hid, "R2", wrong).json()
    assert bad_r2["matched"] is False and bad_r2["completed"] is False
    assert client.get(f"/handoffs/{hid}").json()["status"] == "active"


def test_invalid_digest_rejected(client):
    bid = _ready_batch(client, "handoff-bad-digest")
    hid = _claim(client, bid).json()["handoff_id"]
    assert _confirm(client, hid, "R1", "not-a-digest").status_code == 422
    assert _confirm(client, hid, "R1", "A" * 64).status_code == 200  # 大小写不敏感


def test_reclaim_after_expiry_new_handoff_and_late_confirm_rejected(client, settings):
    bid = _ready_batch(client, "handoff-reclaim")
    old = _claim(client, bid).json()
    old_id = old["handoff_id"]

    # 旧号先到一端
    assert _confirm(client, old_id, "R1", old["r1"]["sha256"]).json()["r1_confirmed"] is True

    # 租期到点后重领：得到新交接号，旧号作废
    _expire_handoff(old_id, settings)
    resp = _claim(client, bid)
    assert resp.status_code == 201
    new = resp.json()
    new_id = new["handoff_id"]
    assert new_id != old_id
    assert new["reused"] is False
    assert new["seconds_remaining"] > 0
    assert new["r1"]["confirmed"] is False and new["r2"]["confirmed"] is False
    assert new["r1"]["sha256"] == old["r1"]["sha256"]

    # 旧号的迟到确认必须被拒绝，且不能改变新交接
    late = _confirm(client, old_id, "R2", old["r2"]["sha256"])
    assert late.status_code == 409
    late_r1 = _confirm(client, old_id, "R1", old["r1"]["sha256"])
    assert late_r1.status_code == 409

    detail = client.get(f"/handoffs/{new_id}").json()
    assert detail["status"] == "active"
    assert detail["r1"]["confirmed"] is False and detail["r2"]["confirmed"] is False

    with get_conn(settings.db_path) as conn:
        old_status = conn.execute(
            "SELECT status FROM handoffs WHERE id = ?", (old_id,)
        ).fetchone()["status"]
        confirmations = conn.execute(
            "SELECT COUNT(*) FROM handoff_confirmations WHERE handoff_id = ?", (new_id,)
        ).fetchone()[0]
    assert old_status == HandoffStatus.EXPIRED.value
    assert confirmations == 0

    # 新交接两端必须重新确认，之后才算完成
    assert _confirm(client, new_id, "R1", new["r1"]["sha256"]).json()["completed"] is False
    assert _confirm(client, new_id, "R2", new["r2"]["sha256"]).json()["completed"] is True


def test_expired_handoff_download_rejected(client, settings):
    bid = _ready_batch(client, "handoff-expired-download")
    body = _claim(client, bid).json()
    _expire_handoff(body["handoff_id"], settings)
    resp = client.get(body["r1"]["download_path"])
    assert resp.status_code == 409
    # 旧批次下载接口仍可用
    assert client.get(f"/batches/{bid}/files/R1/download").status_code == 200


def test_claim_with_missing_file_leaves_no_record(client, settings):
    bid = _ready_batch(client, "handoff-missing-file")
    (settings.batch_dir / bid / "R2.fastq").unlink()
    resp = _claim(client, bid)
    assert resp.status_code == 409
    assert "R2" in resp.json()["detail"]
    with get_conn(settings.db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM handoffs WHERE batch_id = ?", (bid,)
        ).fetchone()[0] == 0


def test_expired_confirm_rejected_and_expiry_persisted(client, settings):
    """到期后未重领：迟到确认 409，且交接状态确实落库为 expired（不是仅 409 回滚）。"""
    bid = _ready_batch(client, "handoff-late-confirm")
    body = _claim(client, bid).json()
    hid = body["handoff_id"]
    _expire_handoff(hid, settings)

    resp = _confirm(client, hid, "R1", body["r1"]["sha256"])
    assert resp.status_code == 409
    with get_conn(settings.db_path) as conn:
        row = conn.execute(
            "SELECT status FROM handoffs WHERE id = ?", (hid,)
        ).fetchone()
        confirmations = conn.execute(
            "SELECT COUNT(*) FROM handoff_confirmations WHERE handoff_id = ?", (hid,)
        ).fetchone()[0]
    assert row["status"] == HandoffStatus.EXPIRED.value
    assert confirmations == 0


def test_completed_record_survives_restart_and_late_confirm_ignored(client, settings):
    bid = _ready_batch(client, "handoff-restart")
    body = _claim(client, bid).json()
    hid = body["handoff_id"]
    _confirm(client, hid, "R1", body["r1"]["sha256"])
    assert _confirm(client, hid, "R2", body["r2"]["sha256"]).json()["completed"] is True
    completed_at = client.get(f"/handoffs/{hid}").json()["completed_at"]

    new_service = BatchService(Settings())
    asyncio.run(new_service.recover_on_startup())

    with get_conn(settings.db_path) as conn:
        row = conn.execute(
            "SELECT status, completed_at, r1_sha256, r2_sha256 FROM handoffs WHERE id = ?",
            (hid,),
        ).fetchone()
    assert row["status"] == "completed"
    assert row["completed_at"] == completed_at
    assert row["r1_sha256"] == body["r1"]["sha256"]
    assert row["r2_sha256"] == body["r2"]["sha256"]
    # ready 批文件保留
    assert (settings.batch_dir / bid / "R1.fastq").exists()

    # 完成后的重复同摘要确认幂等，不改变完成时间
    again = _confirm(client, hid, "R1", body["r1"]["sha256"])
    assert again.status_code == 200 and again.json()["completed"] is True
    # 完成后拿错误摘要翻案被拒
    assert _confirm(client, hid, "R2", "0" * 64).status_code == 409
    assert client.get(f"/handoffs/{hid}").json()["completed_at"] == completed_at


def test_active_handoff_expiry_preserved_across_restart(client, settings):
    bid = _ready_batch(client, "handoff-restart-active")
    body = _claim(client, bid).json()
    hid = body["handoff_id"]
    expires_at = body["expires_at"]
    assert _confirm(client, hid, "R1", body["r1"]["sha256"]).status_code == 200

    new_service = BatchService(Settings())
    summary = asyncio.run(new_service.recover_on_startup())
    assert summary["handoffs"]["active_preserved"] >= 1

    with get_conn(settings.db_path) as conn:
        row = conn.execute(
            "SELECT status, expires_at FROM handoffs WHERE id = ?", (hid,)
        ).fetchone()
    assert row["status"] == "active"
    assert row["expires_at"] == expires_at  # 未完成交接沿用原到期时间

    after = client.get(f"/handoffs/{hid}").json()
    assert after["expires_at"] == expires_at
    assert after["r1"]["confirmed"] is True
    # 重启后仍可继续完成
    assert _confirm(client, hid, "R2", body["r2"]["sha256"]).json()["completed"] is True


def test_ready_batch_not_cancellable_and_handoff_stays_valid(client):
    """ready 是终态：取消不改批次状态，交接照常可确认（原状态语义不变）。"""
    bid = _ready_batch(client, "handoff-stays-ready")
    body = _claim(client, bid).json()
    hid = body["handoff_id"]

    assert client.post(f"/batches/{bid}/cancel").status_code == 409
    assert client.get(f"/batches/{bid}").json()["status"] == "ready"
    assert client.get(body["r1"]["download_path"]).status_code == 200
    assert _confirm(client, hid, "R1", body["r1"]["sha256"]).status_code == 200
    assert _confirm(client, hid, "R2", body["r2"]["sha256"]).json()["completed"] is True


def test_hash_failure_leaves_no_handoff_record(client, settings, monkeypatch):
    bid = _ready_batch(client, "handoff-hash-fail")
    from app import handoff as handoff_module

    def boom(path):
        raise OSError("simulated read failure")

    monkeypatch.setattr(handoff_module, "_hash_file", boom)
    resp = _claim(client, bid)
    assert resp.status_code == 500
    assert "SHA-256" in resp.json()["detail"]
    with get_conn(settings.db_path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM handoffs WHERE batch_id = ?", (bid,)
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT COUNT(*) FROM handoff_confirmations c"
            " JOIN handoffs h ON h.id = c.handoff_id WHERE h.batch_id = ?",
            (bid,),
        ).fetchone()[0] == 0
