#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-$PROJECT_DIR/.venv/bin/python}"

PROCESS_INPUT_FILE="${PROCESS_INPUT_FILE:-/srv/huanggecarbon/input/process/20260511.csv}"
PROCESS_WORK_DIR="${PROCESS_WORK_DIR:-/srv/huanggecarbon/work/process_latest}"
NETWORK_WORK_DIR="${NETWORK_WORK_DIR:-/srv/huanggecarbon/input/network}"
CARBON_API_DATA_DIR="${CARBON_API_DATA_DIR:-$PROJECT_DIR/data}"

echo "== 黄阁碳排自动计算开始：$(date '+%F %T') =="

"$PYTHON" "$PROJECT_DIR/scripts/run_process_calc.py" \
  --input "$PROCESS_INPUT_FILE" \
  --work-dir "$PROCESS_WORK_DIR" \
  --data-dir "$CARBON_API_DATA_DIR" \
  --publish

"$PYTHON" "$PROJECT_DIR/scripts/run_network_calc.py" \
  --work-dir "$NETWORK_WORK_DIR" \
  --data-dir "$CARBON_API_DATA_DIR" \
  --publish

if command -v systemctl >/dev/null 2>&1; then
  if systemctl list-unit-files | grep -q '^huanggecarbon\\.service'; then
    echo "重启 huanggecarbon 服务"
    sudo systemctl restart huanggecarbon
  fi
fi

echo "== 黄阁碳排自动计算完成：$(date '+%F %T') =="
