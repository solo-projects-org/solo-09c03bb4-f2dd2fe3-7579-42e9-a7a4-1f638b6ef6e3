#!/usr/bin/env python3
"""生成三类演示样例：正常、错配、超限。

用法:
    python scripts/make_samples.py [输出目录]

输出目录下:
    normal/R1.fastq.gz  normal/R2.fastq.gz       # 合法双端
    mismatch/R1.fastq.gz mismatch/R2.fastq.gz    # R2 第 2 条读段名与 R1 不一致
    oversized/R1.fastq.gz oversized/R2.fastq.gz  # 展开后远超 64 MiB 上限
    corrupt/R1.fastq.gz                          # 损坏的 gzip（截断）
"""

from __future__ import annotations

import gzip
import os
import sys
from pathlib import Path


def _record(name: bytes, seq: bytes, qual: bytes) -> bytes:
    return b"@" + name + b"\n" + seq + b"\n+\n" + qual + b"\n"


def _pair(index: int, length: int = 80) -> tuple[bytes, bytes]:
    # 确定性生成，避免引入随机依赖
    seq1 = bytes(65 + (i * 7 + index) % 26 for i in range(length))
    seq2 = bytes(65 + (i * 11 + index) % 26 for i in range(length))
    qual = bytes(33 + (i * 3 + index) % 60 for i in range(length))
    base = f"sample-seq-{index:06d}".encode()
    return _record(base + b"/1", seq1, qual), _record(base + b"/2", seq2, qual)


def _write_pair(directory: Path, count: int, *, mismatch_at: int | None = None) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    with gzip.open(directory / "R1.fastq.gz", "wb") as f1, gzip.open(
        directory / "R2.fastq.gz", "wb"
    ) as f2:
        for i in range(count):
            r1, r2 = _pair(i)
            if mismatch_at is not None and i == mismatch_at:
                # 读段名被调包，去掉 /2 后仍与 R1 对不上
                r2 = r2.replace(b"sample-seq-", b"sample-WRONG-", 1)
            f1.write(r1)
            f2.write(r2)


def main(out: Path) -> None:
    # 1) 正常样例：1000 对，约 330 KB 展开数据
    _write_pair(out / "normal", 1000)

    # 2) 错配样例：第 2 条（index=1）读段名不一致
    _write_pair(out / "mismatch", 10, mismatch_at=1)

    # 3) 超限样例：每条记录 4*82≈328 字节，40 万对 ≈ 128 MiB，超过 64 MiB
    _write_pair(out / "oversized", 400_000)

    # 4) 损坏 gzip：合法流被截断
    corrupt_dir = out / "corrupt"
    corrupt_dir.mkdir(parents=True, exist_ok=True)
    tmp_gz = out / "_tmp_corrupt.gz"
    with gzip.open(tmp_gz, "wb") as f:
        for i in range(5):
            r1, _ = _pair(i)
            f.write(r1)
    blob = tmp_gz.read_bytes()
    tmp_gz.unlink()
    (corrupt_dir / "R1.fastq.gz").write_bytes(blob[: len(blob) // 2])

    for path in sorted(out.rglob("*.gz")):
        print(f"{path.relative_to(out)}  {os.path.getsize(path)} bytes")


if __name__ == "__main__":
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "samples")
    main(target)
