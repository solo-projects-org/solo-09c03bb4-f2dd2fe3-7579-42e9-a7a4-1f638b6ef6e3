"""Pydantic 请求与响应模型。"""

from __future__ import annotations

from pydantic import BaseModel, Field

from .models import BatchStatus, FileRole, HandoffStatus


class BatchCreateRequest(BaseModel):
    sample_id: str = Field(min_length=1, max_length=128, description="样本编号")


class FileInfo(BaseModel):
    role: FileRole
    size: int
    uploaded_at: str


class BatchResponse(BaseModel):
    id: str
    sample_id: str
    status: BatchStatus
    byte_limit: int
    bytes_used: int
    error: str | None = None
    files: list[FileInfo] = Field(default_factory=list)
    created_at: str
    updated_at: str


class BatchListResponse(BaseModel):
    batches: list[BatchResponse]


class CancelResponse(BaseModel):
    id: str
    status: BatchStatus
    freed_bytes: int
    message: str


# --------------------------------------------------------------------------- #
# 交接
# --------------------------------------------------------------------------- #


class HandoffEnd(BaseModel):
    role: FileRole
    sha256: str = Field(description="领取时对原档计算出的 SHA-256（十六进制小写）")
    size: int = Field(description="原档字节数")
    download_url: str = Field(description="该端的交接下载入口")
    download_path: str = Field(description="该端交接下载入口的相对路径")
    confirmed: bool = Field(description="worker 是否已回报过该端且摘要吻合")


class HandoffResponse(BaseModel):
    handoff_id: str
    batch_id: str
    sample_id: str
    status: HandoffStatus
    lease_seconds: int
    expires_at: str
    seconds_remaining: int
    r1: HandoffEnd
    r2: HandoffEnd
    downloads: dict[str, str] = Field(
        default_factory=dict, description="role -> 交接下载入口路径"
    )
    completed_at: str | None = None
    reused: bool = Field(
        default=False, description="true 表示命中租期内已存在的交接，而非本次新建"
    )
    message: str


class HandoffConfirmRequest(BaseModel):
    sha256: str = Field(min_length=1, max_length=128, description="worker 实际收到文件的 SHA-256")


class HandoffConfirmResponse(BaseModel):
    handoff_id: str
    batch_id: str
    role: FileRole
    matched: bool
    status: HandoffStatus
    completed: bool
    r1_confirmed: bool
    r2_confirmed: bool
    expires_at: str
    completed_at: str | None = None
    message: str
