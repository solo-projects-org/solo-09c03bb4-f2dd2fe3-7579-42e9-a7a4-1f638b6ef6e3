"""磁盘空间配额管理与批次运行期注册表。"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from .models import BatchRuntime


class QuotaExceededError(Exception):
    """单批累计字节超过配置上限。"""

    def __init__(self, batch_id: str, limit: int, attempted: int, current: int) -> None:
        self.batch_id = batch_id
        self.limit = limit
        self.attempted = attempted
        self.current = current
        super().__init__(
            f"batch {batch_id} byte quota exceeded: "
            f"current={current} attempted={attempted} limit={limit}"
        )


class SpaceManager:
    """按批记账的字节配额。

    记账内容 = 该批数据卷目录下已写入的全部字节（压缩临时文件 + 展开 FASTQ）。
    写入前预留、失败/删除时回收，全程异步锁保护，多批并发互不阻塞但同批严格串行校验。
    """

    def __init__(self, batch_byte_limit: int) -> None:
        self._limit = batch_byte_limit
        self._allocated: dict[str, int] = {}
        self._lock = asyncio.Lock()

    @property
    def limit(self) -> int:
        return self._limit

    async def reserve(self, batch_id: str, additional: int) -> None:
        async with self._lock:
            current = self._allocated.get(batch_id, 0)
            if current + additional > self._limit:
                raise QuotaExceededError(batch_id, self._limit, additional, current)
            self._allocated[batch_id] = current + additional

    async def release(self, batch_id: str, bytes_: int) -> None:
        if bytes_ <= 0:
            return
        async with self._lock:
            current = self._allocated.get(batch_id, 0)
            self._allocated[batch_id] = max(0, current - bytes_)

    async def set_batch_usage(self, batch_id: str, usage: int) -> None:
        """以磁盘真实占用为准，重置该批记账（重启恢复 / 清理后对账用）。"""
        async with self._lock:
            if usage <= 0:
                self._allocated.pop(batch_id, None)
            else:
                self._allocated[batch_id] = usage

    async def remove_batch(self, batch_id: str) -> None:
        async with self._lock:
            self._allocated.pop(batch_id, None)

    async def usage(self) -> dict[str, int]:
        async with self._lock:
            return dict(self._allocated)


def directory_size(path: Path) -> int:
    total = 0
    if not path.exists():
        return 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            fp = Path(root) / name
            try:
                total += fp.stat().st_size
            except FileNotFoundError:
                pass
    return total


def remove_batch_files(batch_dir: Path) -> int:
    """删除批次目录下所有文件并返回释放的字节数。"""
    freed = directory_size(batch_dir)
    if batch_dir.exists():
        for child in batch_dir.iterdir():
            if child.is_file():
                child.unlink()
    return freed


class BatchRegistry:
    """batch_id -> 运行期协调对象。"""

    def __init__(self) -> None:
        self._runtimes: dict[str, BatchRuntime] = {}
        self._lock = asyncio.Lock()

    async def create(self, batch_id: str) -> BatchRuntime:
        async with self._lock:
            rt = self._runtimes.get(batch_id)
            if rt is None:
                rt = BatchRuntime(batch_id=batch_id)
                self._runtimes[batch_id] = rt
            return rt

    async def get(self, batch_id: str) -> BatchRuntime | None:
        return self._runtimes.get(batch_id)

    async def remove(self, batch_id: str) -> None:
        async with self._lock:
            self._runtimes.pop(batch_id, None)
