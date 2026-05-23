#!/usr/bin/env bash
# 从清单文件批量对比 FCT。默认清单: fct_compare_manifest.txt（可复制 example 修改）
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="${1:-$SCRIPT_DIR/fct_compare_manifest.txt}"
DETAIL="${DETAIL:-1}"
TIME_LIMIT="${TIME_LIMIT:-10000000000}"
FLOW_TYPE="${FLOW_TYPE:-0}"

if [[ ! -f "$MANIFEST" ]]; then
  echo "清单不存在: $MANIFEST" >&2
  echo "可复制 fct_compare_manifest.example.txt 为 fct_compare_manifest.txt" >&2
  exit 1
fi

args=()
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="$(echo "$line" | xargs)"
  [[ -z "$line" ]] && continue
  if [[ "$line" != *:* ]]; then
    echo "跳过无效行: $line" >&2
    continue
  fi
  name="${line%%:*}"
  path="${line#*:}"
  if [[ "$path" != /* ]]; then
    path="$SCRIPT_DIR/$path"
  fi
  args+=(--case "${name}:${path}")
done < "$MANIFEST"

cmd=(python3 "$SCRIPT_DIR/fct_standard_report.py" -t "$FLOW_TYPE" -T "$TIME_LIMIT" compare)
[[ "$DETAIL" == "1" ]] && cmd+=(--detail)
cmd+=("${args[@]}")
exec "${cmd[@]}"
