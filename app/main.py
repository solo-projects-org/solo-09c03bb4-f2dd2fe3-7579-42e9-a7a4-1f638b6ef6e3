"""FastAPI 入口：双端 FASTQ.gz 单进程接收服务。"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse

from .config import get_settings
from .models import FileRole
from .schemas import (
    BatchCreateRequest,
    BatchListResponse,
    BatchResponse,
    CancelResponse,
    HandoffConfirmRequest,
    HandoffConfirmResponse,
    HandoffResponse,
)
from .service import BatchService

logger = logging.getLogger("fastq_receiver")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

settings = get_settings()
service = BatchService(settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("data dir: %s, per-batch byte limit: %d", settings.data_dir, settings.batch_byte_limit)
    summary = await service.recover_on_startup()
    logger.info("startup recovery: %s", summary)
    yield


app = FastAPI(
    title="双端 FASTQ.gz 接收 API",
    description=(
        "测序双端数据按样本建批接收。R1、R2 各以请求体流式上传一次，"
        "单成员 gzip、严格四行制 FASTQ，两端条数与读段名逐对校验。"
        "坏包或校验失败整批拒收；单批文件字节上限接收过程中实时守限。"
        "ready 批可领取有租期的交接：领取时对两端原档计算 SHA-256，"
        "比对 worker 按端回报收到的摘要，两端都吻合才算交付完成。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.post("/batches", response_model=BatchResponse, status_code=201, tags=["batches"],
          summary="按样本创建接收批次")
async def create_batch(payload: BatchCreateRequest):
    return await service.create_batch(payload.sample_id)


@app.get("/batches", response_model=BatchListResponse, tags=["batches"], summary="列出全部批次")
async def list_batches():
    return {"batches": await service.list_batches()}


@app.get("/batches/{batch_id}", response_model=BatchResponse, tags=["batches"], summary="查询批次状态")
async def get_batch(batch_id: str):
    return await service.get_batch_detail(batch_id)


@app.post(
    "/batches/{batch_id}/files/{role}",
    response_model=BatchResponse,
    tags=["upload"],
    summary="流式上传 R1 或 R2（请求体即 gzip 字节，不用 multipart）",
    responses={
        409: {"description": "批次非 receiving / 该端已上传 / 批次取消中"},
        413: {"description": "超过单批文件字节上限，本批已失败并清理"},
        422: {"description": "坏 gzip 或 FASTQ 校验失败，本批已拒收并清理"},
    },
)
async def upload_file(batch_id: str, role: FileRole, request: Request):
    return await service.upload(batch_id, role, request)


@app.post("/batches/{batch_id}/cancel", response_model=CancelResponse, tags=["batches"],
          summary="取消批次（等待写入停止、文件删除、空间释放后确认）")
async def cancel_batch(batch_id: str):
    return await service.cancel(batch_id)


@app.get("/batches/{batch_id}/files/{role}/download", tags=["download"],
         summary="下载展开后的 R1 / R2 FASTQ（仅 ready 批次）")
async def download_file(batch_id: str, role: FileRole):
    path, filename = service.download_path(batch_id, role)
    return FileResponse(path, media_type="text/plain", filename=filename)


# --------------------------------------------------------------------------- #
# 有租期的交接
# --------------------------------------------------------------------------- #


@app.post(
    "/batches/{batch_id}/handoffs",
    response_model=HandoffResponse,
    status_code=201,
    tags=["handoff"],
    summary="领取交接（对 R1、R2 原档计算 SHA-256，返回交接号、两端摘要与交接下载入口）",
    responses={
        409: {"description": "批次非 ready / 原档缺失，未产生交接记录"},
        500: {"description": "摘要计算失败，未产生交接记录"},    },
)
async def claim_handoff(batch_id: str):
    return await service.handoffs.claim(batch_id)


@app.get(
    "/handoffs/{handoff_id}",
    response_model=HandoffResponse,
    tags=["handoff"],
    summary="查询交接状态（两端确认情况、租期剩余秒数）",
)
async def get_handoff(handoff_id: str):
    return await service.handoffs.get(handoff_id)


@app.post(
    "/handoffs/{handoff_id}/confirm/{role}",
    response_model=HandoffConfirmResponse,
    tags=["handoff"],
    summary="按端回报实际收到文件的 SHA-256（两端都吻合才算交付完成）",
    responses={
        409: {"description": "交接已过期 / 已完成但摘要不符 / 批次已不可交付"},
        422: {"description": "摘要不是 64 位十六进制"},
    },
)
async def confirm_handoff(handoff_id: str, role: FileRole, payload: HandoffConfirmRequest):
    return await service.handoffs.confirm(handoff_id, role, payload.sha256)


@app.get(
    "/handoffs/{handoff_id}/files/{role}/download",
    tags=["handoff"],
    summary="按交接号下载该端原档（租期内或已完成，过期交接拒绝）",
)
async def download_handoff_file(handoff_id: str, role: FileRole):
    path, filename = service.handoffs.download_path(handoff_id, role)
    return FileResponse(path, media_type="text/plain", filename=filename)


@app.get("/health", tags=["system"], summary="健康检查")
async def health():
    usage = await service.space.usage()
    return {
        "status": "ok",
        "batch_byte_limit": settings.batch_byte_limit,
        "active_batches": len(usage),
        "data_dir": str(settings.data_dir),
    }
