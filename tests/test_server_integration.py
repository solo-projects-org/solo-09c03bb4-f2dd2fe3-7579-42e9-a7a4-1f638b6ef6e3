"""真实 uvicorn 进程集成测试。

TestClient 的 BlockingPortal 各自持有独立事件循环，无法模拟单进程 uvicorn
单事件循环下「取消协程等待上传协程停止」的并发语义，因此这里拉起真实服务器，
用标准库 urllib + 线程驱动，验证端到端行为。
"""

from __future__ import annotations

import gzip
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def server():
    data_dir = Path(tempfile.mkdtemp(prefix="fastq-itg-")) / "data"
    port = _free_port()
    env = {
        **os.environ,
        "FASTQ_DATA_DIR": str(data_dir),
        "FASTQ_BATCH_BYTE_LIMIT": str(64 * 1024 * 1024),
        "PYTHONPATH": str(REPO_ROOT),
    }
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "app.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(base + "/health", timeout=1) as r:
                if r.status == 200:
                    break
        except OSError:
            time.sleep(0.2)
    else:
        proc.terminate()
        out = proc.stdout.read() if proc.stdout else ""
        pytest.fail(f"server did not start:\n{out}")

    yield {"base": base, "data_dir": data_dir, "proc": proc}

    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def _req(server, method: str, path: str, body=None, headers=None):
    req = urllib.request.Request(
        server["base"] + path,
        data=body,
        headers=headers or {},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def _make_gz(records) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as fh:
        for name, seq, qual in records:
            fh.write(b"@" + name + b"\n" + seq + b"\n+\n" + qual + b"\n")
    return buf.getvalue()


def _pair(n: int, length: int = 50):
    r1, r2 = [], []
    for i in range(n):
        seq = bytes(65 + (i * 3 + j) % 26 for j in range(length))
        qual = b"J" * length
        base = f"itg-{i}".encode()
        r1.append((base + b"/1", seq, qual))
        r2.append((base + b"/2", seq, qual))
    return r1, r2


def test_full_lifecycle_normal_mismatch_oversized(server):
    # 正常批
    r1, r2 = _pair(5)
    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "itg-ok"}).encode(),
                   {"Content-Type": "application/json"})
    bid = body["id"]
    assert _req(server, "POST", f"/batches/{bid}/files/R1", _make_gz(r1),
                {"Content-Type": "application/gzip"})[0] == 200
    status, body = _req(server, "POST", f"/batches/{bid}/files/R2", _make_gz(r2),
                        {"Content-Type": "application/gzip"})
    assert status == 200 and body["status"] == "ready"
    with urllib.request.urlopen(server["base"] + f"/batches/{bid}/files/R1/download") as dl:
        assert dl.read().startswith(b"@itg-0/1\n")

    # 错配批
    r1b, r2b = _pair(3)
    r2b[1] = (b"itg-WRONG/2", r2b[1][1], r2b[1][2])
    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "itg-bad"}).encode(),
                   {"Content-Type": "application/json"})
    bad = body["id"]
    _req(server, "POST", f"/batches/{bad}/files/R1", _make_gz(r1b),
         {"Content-Type": "application/gzip"})
    code, body = _req(server, "POST", f"/batches/{bad}/files/R2", _make_gz(r2b),
                      {"Content-Type": "application/gzip"})
    assert code == 422 and "mismatch" in body["detail"]
    _, detail = _req(server, "GET", f"/batches/{bad}")
    assert detail["status"] == "failed"
    assert list((server["data_dir"] / "batches" / bad).iterdir()) == []

    # 超限批：序列映射为 4 种碱基但条数足够多，展开后约 40 万 * 328 ≈ 128 MiB
    big = []
    for i in range(400_000):
        seq = os.urandom(80)
        big.append((f"big-{i}/1".encode(), bytes(65 + b % 4 for b in seq), b"J" * 80))
    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "itg-big"}).encode(),
                   {"Content-Type": "application/json"})
    big_id = body["id"]
    code, body = _req(server, "POST", f"/batches/{big_id}/files/R1", _make_gz(big),
                      {"Content-Type": "application/gzip"})
    assert code == 413
    assert list((server["data_dir"] / "batches" / big_id).iterdir()) == []


def test_slow_upload_does_not_block_other_batch(server):
    r1, r2 = _pair(4)
    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "slow"}).encode(),
                   {"Content-Type": "application/json"})
    slow_id = body["id"]
    blob = _make_gz(r1)

    def slow_upload():
        def gen():
            for i in range(0, len(blob), 32):
                time.sleep(0.1)
                yield blob[i : i + 32]

        req = urllib.request.Request(
            server["base"] + f"/batches/{slow_id}/files/R1",
            data=gen(),
            headers={"Content-Type": "application/gzip"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status

    import threading

    t = threading.Thread(target=slow_upload)
    t.start()
    time.sleep(0.3)

    # 慢上传进行中，另一个批次完整走通
    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "fast"}).encode(),
                   {"Content-Type": "application/json"})
    fast_id = body["id"]
    f1, f2 = _pair(4)
    _req(server, "POST", f"/batches/{fast_id}/files/R1", _make_gz(f1),
         {"Content-Type": "application/gzip"})
    code, body = _req(server, "POST", f"/batches/{fast_id}/files/R2", _make_gz(f2),
                      {"Content-Type": "application/gzip"})
    assert code == 200 and body["status"] == "ready"
    t.join(timeout=30)


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def test_handoff_claim_confirm_and_restart(server):
    """真实单进程服务器：并发领取同一交接 → 两端摘要确认 → 重启后记录仍完成。"""
    import threading

    r1, r2 = _pair(6)
    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "itg-handoff"}).encode(),
                   {"Content-Type": "application/json"})
    bid = body["id"]
    _req(server, "POST", f"/batches/{bid}/files/R1", _make_gz(r1),
         {"Content-Type": "application/gzip"})
    assert _req(server, "POST", f"/batches/{bid}/files/R2", _make_gz(r2),
                {"Content-Type": "application/gzip"})[0] == 200

    results = []

    def claim():
        results.append(_req(server, "POST", f"/batches/{bid}/handoffs"))

    threads = [threading.Thread(target=claim) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(code == 201 for code, _ in results), results
    ids = {body["handoff_id"] for _, body in results}
    assert len(ids) == 1, ids
    handoff = results[0][1]
    hid = handoff["handoff_id"]

    bdir = server["data_dir"] / "batches" / bid
    assert handoff["r1"]["sha256"] == _sha256(bdir / "R1.fastq")
    assert handoff["r2"]["sha256"] == _sha256(bdir / "R2.fastq")

    # 交接下载入口真实取到原档
    with urllib.request.urlopen(server["base"] + handoff["r1"]["download_path"]) as dl:
        assert dl.read() == (bdir / "R1.fastq").read_bytes()

    code, body = _req(server, "POST", f"/handoffs/{hid}/confirm/R1",
                      json.dumps({"sha256": handoff["r1"]["sha256"]}).encode(),
                      {"Content-Type": "application/json"})
    assert code == 200 and body["completed"] is False and body["r1_confirmed"] is True
    code, body = _req(server, "POST", f"/handoffs/{hid}/confirm/R2",
                      json.dumps({"sha256": handoff["r2"]["sha256"]}).encode(),
                      {"Content-Type": "application/json"})
    assert code == 200 and body["completed"] is True and body["status"] == "completed"

    # 重启服务器：completed 记录与 ready 批下载都保留
    proc = server["proc"]
    proc.terminate()
    proc.wait(timeout=10)
    env = {
        **os.environ,
        "FASTQ_DATA_DIR": str(server["data_dir"]),
        "FASTQ_BATCH_BYTE_LIMIT": str(64 * 1024 * 1024),
        "PYTHONPATH": str(REPO_ROOT),
    }
    port = int(server["base"].rsplit(":", 1)[1])
    server["proc"] = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1",
         "--port", str(port), "--log-level", "warning"],
        cwd=REPO_ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(server["base"] + "/health", timeout=1) as r:
                if r.status == 200:
                    break
        except OSError:
            time.sleep(0.2)
    else:
        pytest.fail("server did not restart")

    code, detail = _req(server, "GET", f"/handoffs/{hid}")
    assert code == 200 and detail["status"] == "completed"
    assert detail["r1"]["confirmed"] and detail["r2"]["confirmed"]
    # 完成记录的存在不改变原 ready 状态与旧下载接口
    assert _req(server, "GET", f"/batches/{bid}")[1]["status"] == "ready"
    with urllib.request.urlopen(server["base"] + f"/batches/{bid}/files/R2/download") as dl:
        assert dl.read() == (bdir / "R2.fastq").read_bytes()

    # 单成员、不可压缩的大 gzip：随机质量串让压缩后仍有足够体积，慢速传输确保取消时在途
    records = []
    for i in range(2000):
        seq = bytes(65 + b % 4 for b in os.urandom(120))
        qual = bytes(33 + b % 60 for b in os.urandom(120))
        records.append((f"cancel-{i}/1".encode(), seq, qual))
    blob = _make_gz(records)
    assert len(blob) > 100_000

    _, body = _req(server, "POST", "/batches", json.dumps({"sample_id": "cancel"}).encode(),
                   {"Content-Type": "application/json"})
    bid = body["id"]
    bdir = server["data_dir"] / "batches" / bid
    result = {}

    def slow_upload():
        def gen():
            for i in range(0, len(blob), 64):
                time.sleep(0.02)
                yield blob[i : i + 64]

        req = urllib.request.Request(
            server["base"] + f"/batches/{bid}/files/R1",
            data=gen(),
            headers={"Content-Type": "application/gzip"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                result["status"] = r.status
        except urllib.error.HTTPError as e:
            result["status"] = e.code
        except urllib.error.URLError:
            # 取消后服务端关闭连接，发送端收到 broken pipe / connection reset，属预期
            result["status"] = "connection-closed"

    import threading

    t = threading.Thread(target=slow_upload)
    t.start()
    time.sleep(0.4)
    # 此时已有落盘
    assert any(bdir.iterdir())

    code, body = _req(server, "POST", f"/batches/{bid}/cancel")
    assert code == 200 and body["status"] == "cancelled", body

    # 取消确认时：写入已停止、文件已删、空间已释放
    assert list(bdir.iterdir()) == []
    t.join(timeout=30)
    assert not t.is_alive()
    assert result["status"] in (409, 422, "connection-closed")
    # 上传彻底结束后仍无任何文件落盘
    assert list(bdir.iterdir()) == []

    # 取消后不能再上传
    code, _ = _req(server, "POST", f"/batches/{bid}/files/R1", blob,
                   {"Content-Type": "application/gzip"})
    assert code == 409
    assert list(bdir.iterdir()) == []
