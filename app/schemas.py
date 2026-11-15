"""Pydantic 请求与响应模型。"""

from __future__ import annotations

from pydantic import BaseModel, Field

from .models import BatchStatus, FileRole


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
