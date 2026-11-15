"""pytest 夹具：隔离的数据卷 + ASGI 测试客户端。"""

from __future__ import annotations

import gzip
import os
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# 必须在导入 app 之前设置环境变量（pydantic-settings 在导入时读取）
_TMP = tempfile.mkdtemp(prefix="fastq-test-")
os.environ["FASTQ_DATA_DIR"] = str(Path(_TMP) / "data")
os.environ["FASTQ_BATCH_BYTE_LIMIT"] = str(64 * 1024 * 1024)

from app.config import get_settings  # noqa: E402
from app.main import app, service  # noqa: E402


@pytest.fixture(scope="session")
def make_gz():
    def _make(path: Path, records: list[tuple[bytes, bytes, bytes]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wb") as fh:
            for name, seq, qual in records:
                fh.write(b"@" + name + b"\n" + seq + b"\n+\n" + qual + b"\n")

    return _make


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture
def settings():
    return get_settings()


@pytest.fixture
def svc():
    return service
