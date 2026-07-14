#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_PARENT="$(cd "$PROJECT_DIR/.." && pwd)"
PYTHON="${PYTHON:-$PROJECT_DIR/.venv/bin/python}"

DEFAULT_PROCESS_INPUT="$PROJECT_PARENT/中台一年历史数据/20260511.csv"
DEFAULT_PROCESS_WORK_DIR="$PROJECT_PARENT/中台一年历史数据"
DEFAULT_NETWORK_WORK_DIR="$PROJECT_PARENT/管网计算"

if [[ ! -f "$DEFAULT_PROCESS_INPUT" ]]; then
  DEFAULT_PROCESS_INPUT="/srv/huanggecarbon/input/process/20260511.csv"
fi
if [[ ! -d "$DEFAULT_PROCESS_WORK_DIR" ]]; then
  DEFAULT_PROCESS_WORK_DIR="/srv/huanggecarbon/work/process_latest"
fi
if [[ ! -d "$DEFAULT_NETWORK_WORK_DIR" ]]; then
  DEFAULT_NETWORK_WORK_DIR="/srv/huanggecarbon/input/network"
fi

PROCESS_INPUT_FILE="${PROCESS_INPUT_FILE:-$DEFAULT_PROCESS_INPUT}"
PROCESS_WORK_DIR="${PROCESS_WORK_DIR:-$DEFAULT_PROCESS_WORK_DIR}"
NETWORK_WORK_DIR="${NETWORK_WORK_DIR:-$DEFAULT_NETWORK_WORK_DIR}"
CARBON_API_DATA_DIR="${CARBON_API_DATA_DIR:-$PROJECT_DIR/data}"

echo "== 黄阁碳排自动计算开始：$(date '+%F %T') =="
echo "工艺段原始 CSV：$PROCESS_INPUT_FILE"
echo "工艺段工作目录：$PROCESS_WORK_DIR"
echo "管网工作目录：$NETWORK_WORK_DIR"
echo "API data 目录：$CARBON_API_DATA_DIR"

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
