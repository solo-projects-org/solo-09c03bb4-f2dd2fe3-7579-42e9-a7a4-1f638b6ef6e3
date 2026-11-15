#!/usr/bin/env bash
# 端到端演示：正常 / 错配 / 超限三类样例。
# 依赖: curl；样例先由 scripts/make_samples.py 生成。
set -euo pipefail

BASE="${BASE_URL:-http://127.0.0.1:8000}"
SAMPLES="${SAMPLES_DIR:-samples}"

post_batch() {  # $1 = sample_id
  curl -sS -X POST "$BASE/batches" \
    -H 'Content-Type: application/json' \
    -d "{\"sample_id\":\"$1\"}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'
}

upload() {  # $1 = batch_id $2 = R1|R2 $3 = 文件
  echo "  upload $2 ($(wc -c < "$3") compressed bytes)"
  curl -sS -o /tmp/fastq_resp.json -w "  HTTP %{http_code}\n" \
    -X POST "$BASE/batches/$1/files/$2" \
    -H 'Content-Type: application/gzip' \
    --data-binary "@$3"
  python3 -c 'import json; d=json.load(open("/tmp/fastq_resp.json")); print("  ->", d.get("status"), d.get("error") or d.get("detail") or "")'
}

run_case() {  # $1 = 名称 $2 = 样例目录 $3 = 期望状态
  echo "================ $1 ================"
  local bid
  bid="$(post_batch "demo-$1")"
  echo "batch: $bid"
  upload "$bid" R1 "$SAMPLES/$2/R1.fastq.gz"
  if [[ -f "$SAMPLES/$2/R2.fastq.gz" ]]; then
    upload "$bid" R2 "$SAMPLES/$2/R2.fastq.gz"
  fi
  echo "final state:"
  curl -sS "$BASE/batches/$bid" | python3 -m json.tool
}

echo "服务: $BASE"
curl -sS "$BASE/health" | python3 -m json.tool

run_case "normal" "normal"
run_case "mismatch" "mismatch"
run_case "oversized" "oversized"

echo
echo "下载正常批展开后的 R1（前 2 行）:"
NORMAL_BID="$(curl -sS "$BASE/batches" | python3 -c \
  'import json,sys; print(next(b["id"] for b in json.load(sys.stdin)["batches"] if b["status"]=="ready"))')"
curl -sS "$BASE/batches/$NORMAL_BID/files/R1/download" | head -n 2 || true
