"""端到端接口测试。"""

from __future__ import annotations

import gzip

from tests.helpers import create_batch as _create_batch
from tests.helpers import good_pair
from tests.helpers import gz_bytes as _gz
from tests.helpers import upload_bytes as _upload_bytes


def test_health_and_swagger(client):
    assert client.get("/health").status_code == 200
    assert client.get("/docs").status_code == 200
    assert client.get("/openapi.json").status_code == 200


def test_happy_path_ready_and_download(client, make_gz, settings):
    r1, r2 = good_pair()
    bid = _create_batch(client)
    assert _upload_bytes(client, bid, "R1", _gz(r1)).status_code == 200
    resp = _upload_bytes(client, bid, "R2", _gz(r2))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "ready"
    assert len(body["files"]) == 2
    assert body["bytes_used"] > 0

    detail = client.get(f"/batches/{bid}").json()
    assert detail["status"] == "ready"

    dl = client.get(f"/batches/{bid}/files/R1/download")
    assert dl.status_code == 200
    assert dl.content.startswith(b"@read-0/1\n")
    # 压缩临时文件应已删除，只剩展开的 FASTQ
    bdir = settings.batch_dir / bid
    assert sorted(p.name for p in bdir.iterdir()) == ["R1.fastq", "R2.fastq"]


def test_duplicate_role_rejected(client, make_gz):
    r1, _ = good_pair()
    bid = _create_batch(client)
    assert _upload_bytes(client, bid, "R1", _gz(r1)).status_code == 200
    resp = _upload_bytes(client, bid, "R1", _gz(r1))
    assert resp.status_code == 409


def test_mismatched_read_names_reject_whole_batch(client, settings):
    r1, r2 = good_pair()
    r2[1] = (b"read-WRONG/2", r2[1][1], r2[1][2])
    bid = _create_batch(client)
    assert _upload_bytes(client, bid, "R1", _gz(r1)).status_code == 200
    resp = _upload_bytes(client, bid, "R2", _gz(r2))
    assert resp.status_code == 422
    assert "mismatch" in resp.json()["detail"]

    detail = client.get(f"/batches/{bid}").json()
    assert detail["status"] == "failed"
    # 整批文件必须清掉
    assert list((settings.batch_dir / bid).iterdir()) == []

    # 失败批不能再上传 / 下载
    assert _upload_bytes(client, bid, "R2", _gz(r2)).status_code == 409
    assert client.get(f"/batches/{bid}/files/R1/download").status_code == 409


def test_different_record_counts_rejected(client):
    r1, r2 = good_pair(3)
    r2 = r2[:2]
    bid = _create_batch(client)
    _upload_bytes(client, bid, "R1", _gz(r1))
    resp = _upload_bytes(client, bid, "R2", _gz(r2))
    assert resp.status_code == 422
    assert "different record counts" in resp.json()["detail"]


def test_seq_quality_length_mismatch_rejected(client):
    r1, r2 = good_pair()
    name, seq, qual = r1[0]
    r1[0] = (name, seq + b"AA", qual)
    bid = _create_batch(client)
    resp = _upload_bytes(client, bid, "R1", _gz(r1))
    assert resp.status_code == 422
    assert "length" in resp.json()["detail"]


def test_corrupt_gzip_rejected(client, settings):
    good = _gz(good_pair()[0])
    bid = _create_batch(client)
    resp = _upload_bytes(client, bid, "R1", good[: len(good) // 2])
    assert resp.status_code == 422
    assert client.get(f"/batches/{bid}").json()["status"] == "failed"
    assert list((settings.batch_dir / bid).iterdir()) == []


def test_multi_member_gzip_rejected(client):
    import io

    buf = io.BytesIO()
    r1 = good_pair()[0]
    with gzip.GzipFile(fileobj=buf, mode="wb") as fh:
        for rec in r1[:2]:
            fh.write(b"@" + rec[0] + b"\n" + rec[1] + b"\n+\n" + rec[2] + b"\n")
    first = buf.getvalue()
    buf2 = io.BytesIO()
    with gzip.GzipFile(fileobj=buf2, mode="wb") as fh:
        rec = r1[2]
        fh.write(b"@" + rec[0] + b"\n" + rec[1] + b"\n+\n" + rec[2] + b"\n")
    bid = _create_batch(client)
    resp = _upload_bytes(client, bid, "R1", first + buf2.getvalue())
    assert resp.status_code == 422
    assert "multi-member" in resp.json()["detail"]


def test_quota_exceeded_mid_stream_batch_cleaned(client, settings):
    from app.main import service

    # 把上限压到 4 KiB，构造足够大的合法数据
    original_limit = service.space._limit
    service.space._limit = 4096
    try:
        r1, _ = good_pair(n=400, length=80)  # 展开后约 400 * 328 ≈ 128 KiB
        bid = _create_batch(client)
        resp = _upload_bytes(client, bid, "R1", _gz(r1))
        assert resp.status_code == 413
        detail = client.get(f"/batches/{bid}").json()
        assert detail["status"] == "failed"
        assert list((settings.batch_dir / bid).iterdir()) == []
        # 配额已回收
        assert service.space._allocated.get(bid) in (None, 0)
    finally:
        service.space._limit = original_limit


def test_concurrent_batches_isolated(client, settings):
    """多批并行：一批失败不影响另一批通过。"""
    good1, good2 = good_pair()
    bad2 = list(good2)
    bad2[0] = (b"read-WRONG/2", bad2[0][1], bad2[0][2])

    bid_ok = _create_batch(client, "parallel-ok")
    bid_bad = _create_batch(client, "parallel-bad")

    _upload_bytes(client, bid_ok, "R1", _gz(good1))
    bad_resp = _upload_bytes(client, bid_bad, "R1", _gz(good1))
    assert bad_resp.status_code == 200
    bad_resp = _upload_bytes(client, bid_bad, "R2", _gz(bad2))
    assert bad_resp.status_code == 422

    ok_resp = _upload_bytes(client, bid_ok, "R2", _gz(good2))
    assert ok_resp.status_code == 200
    assert ok_resp.json()["status"] == "ready"
    assert client.get(f"/batches/{bid_bad}").json()["status"] == "failed"


def test_cancel_wait_until_clean_and_no_more_writes(client, settings):
    r1, _ = good_pair()
    bid = _create_batch(client)
    r = client.post(f"/batches/{bid}/cancel")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "cancelled"

    detail = client.get(f"/batches/{bid}").json()
    assert detail["status"] == "cancelled"
    assert list((settings.batch_dir / bid).iterdir()) == []

    # 取消后不能再落盘
    resp = _upload_bytes(client, bid, "R1", _gz(r1))
    assert resp.status_code == 409
    assert list((settings.batch_dir / bid).iterdir()) == []
    # 重复取消
    assert client.post(f"/batches/{bid}/cancel").status_code == 409


def test_not_found_and_bad_role(client):
    assert client.get("/batches/nope").status_code == 404
    assert client.post("/batches/nope/files/R1", content=b"x").status_code == 404
    bid = _create_batch(client)
    assert client.post(f"/batches/{bid}/files/R3", content=b"x").status_code == 422


def test_list_batches(client):
    _create_batch(client, "sample-list")
    r = client.get("/batches")
    assert r.status_code == 200
    assert any(b["sample_id"] == "sample-list" for b in r.json()["batches"])
