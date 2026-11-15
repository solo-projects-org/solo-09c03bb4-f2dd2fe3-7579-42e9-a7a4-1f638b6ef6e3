"""流式 gzip 解压与四行制 FASTQ 校验。

所有落盘字节（压缩临时文件 + 展开结果）在写入前都经过 SpaceManager 预留，
因此解压炸弹 / 超大压缩包会在接收过程中触发配额错误，而不是等落盘后才发现。
"""

from __future__ import annotations

import zlib
from pathlib import Path
from typing import BinaryIO, Iterator

from .storage import QuotaExceededError, SpaceManager


class StreamError(Exception):
    """上传流处理过程中的可预期错误（坏 gzip / 客户端中断等）。"""


async def stream_gunzip(
    *,
    request,
    gz_path: Path,
    out_path: Path,
    space: SpaceManager,
    batch_id: str,
    cancel_event,
    idle_timeout: float = 15.0,
) -> int:
    """读取请求体，一边把原始 gzip 字节写入临时文件，一边流式展开到 out_path。

    两条落盘流都按真实字节数向 SpaceManager 预留空间。
    返回展开后的总字节数。取消 / 坏包 / 超限 / 静默超时均抛异常，由调用方清理本批。
    """
    import asyncio

    import aiofiles

    decomp = zlib.decompressobj(16 + zlib.MAX_WBITS)
    out_written = 0
    got_any = False

    gz_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    body_stream = request.stream()
    cancelled: list[bool] = [False]

    async def _next_chunk():
        # 每个块最多等 idle_timeout 秒，客户端长时间静默即中断本批
        chunk = await asyncio.wait_for(body_stream.__anext__(), timeout=idle_timeout)
        # 取消信号在文件上下文之外处理，保证 aiofiles 句柄能正常关闭
        if cancel_event.is_set():
            cancelled[0] = True
        return chunk

    try:
        async with aiofiles.open(gz_path, "wb") as gz_f, aiofiles.open(out_path, "wb") as out_f:
            while True:
                try:
                    chunk = await _next_chunk()
                except StopAsyncIteration:
                    break
                except asyncio.TimeoutError as exc:
                    raise StreamError(
                        f"no data received within {idle_timeout}s, upload aborted"
                    ) from exc

                if cancelled[0]:
                    break
                if not chunk:
                    continue
                got_any = True

                # 1) 压缩字节先预留再落盘
                await space.reserve(batch_id, len(chunk))
                try:
                    await gz_f.write(chunk)
                except BaseException:
                    await space.release(batch_id, len(chunk))
                    raise

                # 2) 流式解压；坏 gzip 在此抛 zlib.error
                try:
                    plain = decomp.decompress(chunk)
                except zlib.error as exc:
                    raise StreamError(f"corrupt gzip stream: {exc}") from exc

                if plain:
                    await space.reserve(batch_id, len(plain))
                    try:
                        await out_f.write(plain)
                        out_written += len(plain)
                    except BaseException:
                        await space.release(batch_id, len(plain))
                        raise
    except QuotaExceededError:
        raise
    except StreamError:
        raise
    except Exception as exc:  # 客户端断连等
        raise StreamError(f"upload stream interrupted: {exc}") from exc

    # 文件句柄已正常关闭，再抛出取消信号
    if cancelled[0]:
        raise StreamError("batch cancelled while receiving")

    if not got_any:
        raise StreamError("empty request body")

    # 3) 必须到达 gzip 流正常结束点
    try:
        tail = decomp.flush()
    except zlib.error as exc:
        raise StreamError(f"corrupt gzip stream: {exc}") from exc
    if tail:
        await space.reserve(batch_id, len(tail))
        async with aiofiles.open(out_path, "ab") as out_f:
            await out_f.write(tail)
        out_written += len(tail)
    if not decomp.eof:
        raise StreamError("truncated gzip stream: missing end-of-stream marker")

    # 4) 只收单成员 gzip：结束标记之后不允许还有数据
    if decomp.unused_data:
        raise StreamError("multi-member gzip archives are not accepted")

    return out_written


# --------------------------------------------------------------------------- #
# FASTQ 校验
# --------------------------------------------------------------------------- #


def _read_lines(fh: BinaryIO) -> Iterator[bytes]:
    """逐行生成去掉行尾换行的字节行；文件未以换行结尾视为损坏。"""
    while True:
        raw = fh.readline()
        if not raw:
            return
        if not raw.endswith(b"\n"):
            raise ValueError("file does not end with newline")
        yield raw[:-2] if raw.endswith(b"\r\n") else raw[:-1]


def _next_line(lines: Iterator[bytes], message: str) -> bytes:
    try:
        return next(lines)
    except StopIteration:
        raise ValueError(message) from None


def _iter_records(lines: Iterator[bytes], side: str) -> Iterator[tuple[bytes, bytes, bytes]]:
    """把四行制行流转成 (header, seq, qual) 记录流，并做单端结构校验。"""
    index = 0
    while True:
        try:
            header = next(lines)
        except StopIteration:
            return
        seq = _next_line(lines, f"{side} truncated: missing sequence at record {index + 1}")
        plus = _next_line(lines, f"{side} truncated: missing '+' line at record {index + 1}")
        qual = _next_line(lines, f"{side} truncated: missing quality at record {index + 1}")

        if not header.startswith(b"@"):
            raise ValueError(f"{side} record {index + 1}: header must start with '@'")
        if not seq:
            raise ValueError(f"{side} record {index + 1}: empty sequence")
        if plus != b"+":
            raise ValueError(f"{side} record {index + 1}: third line must be '+'")
        if len(seq) != len(qual):
            raise ValueError(
                f"{side} record {index + 1}: sequence length {len(seq)} "
                f"!= quality length {len(qual)}"
            )
        yield header, seq, qual
        index += 1


def _base_name(header_line: bytes, suffix: bytes) -> bytes:
    """取读段名（首个空白前的 token）并去掉末尾的 /1 或 /2。"""
    token = header_line[1:].split(None, 1)[0]
    if token.endswith(suffix):
        token = token[: -len(suffix)]
    return token


def validate_fastq_structure(path: Path) -> int:
    """校验单个文件为严格四行制 FASTQ，返回读段条数。"""
    count = 0
    with open(path, "rb") as fh:
        for _header, _seq, _qual in _iter_records(_read_lines(fh), path.name):
            count += 1
    if count == 0:
        raise ValueError("FASTQ contains no records")
    return count


def validate_fastq_pair(r1_path: Path, r2_path: Path) -> int:
    """两端交叉校验：条数相同、逐对读段名去 /1 /2 后一致、序列与质量等长。

    返回读段对数量。
    """
    pairs = 0
    with open(r1_path, "rb") as f1, open(r2_path, "rb") as f2:
        rec1 = _iter_records(_read_lines(f1), "R1")
        rec2 = _iter_records(_read_lines(f2), "R2")
        while True:
            r1: tuple[bytes, bytes, bytes] | None = next(rec1, None)
            r2: tuple[bytes, bytes, bytes] | None = next(rec2, None)
            if r1 is None and r2 is None:
                break
            if r1 is None or r2 is None:
                raise ValueError("R1 and R2 have different record counts")
            h1, _s1, _q1 = r1
            h2, _s2, _q2 = r2
            if _base_name(h1, b"/1") != _base_name(h2, b"/2"):
                raise ValueError(
                    f"record {pairs + 1}: read name mismatch after stripping /1 /2: "
                    f"{h1!r} vs {h2!r}"
                )
            pairs += 1

    if pairs == 0:
        raise ValueError("FASTQ pair contains no records")
    return pairs
