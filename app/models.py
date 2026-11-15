"""批次状态模型与运行期注册表。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class BatchStatus(str, Enum):
    RECEIVING = "receiving"
    READY = "ready"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FileRole(str, Enum):
    R1 = "R1"
    R2 = "R2"


class HandoffStatus(str, Enum):
    """有租期的交接状态。

    ACTIVE     租期内可下载、可按端回报摘要；
    COMPLETED  两端摘要均已吻合，交付完成，记录永久保留；
    EXPIRED    租期已过或被同批新交接取代，旧交接号不再接受确认。
    """

    ACTIVE = "active"
    COMPLETED = "completed"
    EXPIRED = "expired"


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def iso_after(seconds: float) -> str:
    """当前时间 + seconds 秒的 ISO 时间串（与 utcnow_iso 同格式，可直接字符串比较）。"""
    return datetime.fromtimestamp(
        datetime.now(timezone.utc).timestamp() + seconds, timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def iso_seconds_remaining(deadline: str) -> int:
    """把 ISO 截止时间换算成剩余秒数（向上取整，已过期返回 0）。"""
    due = datetime.strptime(deadline, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    delta = (due - datetime.now(timezone.utc)).total_seconds()
    return max(0, int(-(-delta // 1)))


@dataclass
class ActiveUpload:
    role: FileRole
    task: asyncio.Task


@dataclass
class BatchRuntime:
    """内存中的批次协调对象：取消事件与正在进行的上传任务。"""

    batch_id: str
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    uploads: dict[FileRole, asyncio.Task] = field(default_factory=dict)
