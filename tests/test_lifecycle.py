"""重启恢复、配额记账与取消中断的单元/集成测试。"""

from __future__ import annotations

import asyncio
import os

import pytest

from app.config import Settings
from app.database import get_conn
from app.service import BatchService
from app.storage import directory_size
from app.validation import StreamError, stream_gunzip
from tests.helpers import create_batch as _create_batch
from tests.helpers import good_pair
from tests.helpers import gz_bytes as _gz
from tests.helpers import upload_bytes as _upload_bytes


def test_restart_keeps_ready_and_aborts_receiving(client, settings):
    # 准备一个 ready 批和一个只传了 R1 的 receiving 批
    r1, r2 = good_pair()
    ready_id = _create_batch(client, "persist-ready")
    _upload_bytes(client, ready_id, "R1", _gz(r1))
    _upload_bytes(client, ready_id, "R2", _gz(r2))
    assert client.get(f"/batches/{ready_id}").json()["status"] == "ready"

    receiving_id = _create_batch(client, "persist-open")
    _upload_bytes(client, receiving_id, "R1", _gz(r1))

    # 模拟重启：用同一数据卷新建 service 并执行启动恢复
    new_service = BatchService(Settings())
    summary = asyncio.run(new_service.recover_on_startup())
    assert summary["ready"] >= 1
    assert summary["aborted"] >= 1

    with get_conn(settings.db_path) as conn:
        ready_row = conn.execute(
            "SELECT status FROM batches WHERE id = ?", (ready_id,)
        ).fetchone()
        open_row = conn.execute(
            "SELECT status FROM batches WHERE id = ?", (receiving_id,)
        ).fetchone()
    assert ready_row["status"] == "ready"
    assert open_row["status"] == "failed"

    # ready 批的展开文件仍在，receiving 批文件已回收
    assert (settings.batch_dir / ready_id / "R1.fastq").exists()
    assert list((settings.batch_dir / receiving_id).iterdir()) == []

    # 新 service 的配额账与磁盘一致
    usage = asyncio.run(new_service.space.usage())
    assert usage[ready_id] == directory_size(settings.batch_dir / ready_id)


class _ChunkedRequest:
    """模拟分块、块间让出事件循环的请求体。"""

    def __init__(self, chunks):
        self._chunks = chunks

    def stream(self):
        chunks = self._chunks

        async def _gen():
            for chunk in chunks:
                await asyncio.sleep(0)
                yield chunk

        return _gen()


def test_cancel_signal_interrupts_stream_and_closes_files(client, settings):
    """取消信号在接收途中触发：流接收抛 StreamError 且文件句柄正常关闭可删除。

    端到端的「取消等待写入停止→删文件→释放空间」语义在
    test_server_integration.py 用真实 uvicorn 进程覆盖（TestClient 的
    BlockingPortal 各自持有独立事件循环，无法模拟单 loop 下的协程并发等待）。
    """
    from app.main import service

    bid = _create_batch(client, "cancel-unit")
    runtime = asyncio.run(service.registry.create(bid))

    gz = _gz(good_pair(n=3, length=10)[0])
    chunks = [gz[i : i + 16] for i in range(0, len(gz), 16)]
    gz_path = settings.batch_dir / bid / "R1.fastq.gz.uploading"
    out_path = settings.batch_dir / bid / "R1.fastq"

    async def scenario():
        async def cancel_soon():
            runtime.cancel_event.set()

        asyncio.ensure_future(cancel_soon())
        with pytest.raises(StreamError, match="cancelled"):
            await stream_gunzip(
                request=_ChunkedRequest(chunks),
                gz_path=gz_path,
                out_path=out_path,
                space=service.space,
                batch_id=bid,
                cancel_event=runtime.cancel_event,
                idle_timeout=5.0,
            )

    asyncio.run(scenario())

    # 文件句柄必须已经正常关闭（能直接删除），没有 executor 关闭后的残留
    for name in os.listdir(settings.batch_dir / bid):
        (settings.batch_dir / bid / name).unlink()


def test_quota_accounts_for_compressed_and_decompressed(client, settings):
    """压缩临时文件和展开 FASTQ 同时计入上限。"""
    from app.main import service

    # 极小上限：哪怕压缩后很小，压缩字节 + 展开字节也会超
    original = service.space._limit
    service.space._limit = 256
    try:
        r1, _ = good_pair(n=20, length=40)
        bid = _create_batch(client, "quota-accounting")
        resp = _upload_bytes(client, bid, "R1", _gz(r1))
        assert resp.status_code == 413
        assert client.get(f"/batches/{bid}").json()["status"] == "failed"
    finally:
        service.space._limit = original
