"""测试辅助函数。"""

from __future__ import annotations

import gzip
import io


def good_pair(n: int = 3, length: int = 12) -> tuple[list, list]:
    r1, r2 = [], []
    for i in range(n):
        seq = bytes(65 + (i + j) % 26 for j in range(length))
        qual = b"J" * length
        base = f"read-{i}".encode()
        r1.append((base + b"/1", seq, qual))
        r2.append((base + b"/2", seq, qual))
    return r1, r2


def gz_bytes(records) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as fh:
        for name, seq, qual in records:
            fh.write(b"@" + name + b"\n" + seq + b"\n+\n" + qual + b"\n")
    return buf.getvalue()


def create_batch(client, sample_id: str = "sample-A") -> str:
    r = client.post("/batches", json={"sample_id": sample_id})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def upload_bytes(client, batch_id: str, role: str, blob: bytes):
    return client.post(
        f"/batches/{batch_id}/files/{role}",
        content=blob,
        headers={"Content-Type": "application/gzip"},
    )
