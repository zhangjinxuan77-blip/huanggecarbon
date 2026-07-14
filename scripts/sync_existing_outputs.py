# -*- coding: utf-8 -*-
"""Publish existing calculation outputs into the FastAPI data directory.

Use this when the process and network folders already contain calculated
outputs, and you only need to refresh the API data files without rerunning
the full calculation.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
API_NETWORK_XLSX = "管网碳排_按压力监测点_坐标匹配.xlsx"


def _default_process_dir() -> str:
    local_dir = PROJECT_DIR.parent / "中台一年历史数据"
    if local_dir.exists():
        return str(local_dir)
    return "/srv/huanggecarbon/work/process_latest"


def _default_network_dir() -> str:
    local_dir = PROJECT_DIR.parent / "管网计算"
    if local_dir.exists():
        return str(local_dir)
    return "/srv/huanggecarbon/input/network"


def _copytree_replace(src: Path, dst: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(f"未找到目录：{src}")
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns(".DS_Store", "__pycache__"))


def _copy_history(process_dir: Path, data_dir: Path) -> None:
    history_src = process_dir / "report_history"
    history_dst = data_dir / "history"
    required = [
        "scope123_daily.csv",
        "process_stage_daily.csv",
        "process_unit_daily.csv",
    ]
    missing = [name for name in required if not (history_src / name).exists()]
    if missing:
        raise FileNotFoundError("report_history 缺少文件：" + ", ".join(missing))

    history_dst.mkdir(parents=True, exist_ok=True)
    for name in required:
        shutil.copy2(history_src / name, history_dst / name)


def _find_network_xlsx(network_dir: Path) -> Path:
    direct_candidates = [
        network_dir / API_NETWORK_XLSX,
        network_dir / "管网压力流量区域匹配" / API_NETWORK_XLSX,
        network_dir / "管网碳排结果_按压力监测点_坐标匹配.xlsx",
    ]
    for path in direct_candidates:
        if path.exists():
            return path

    candidates = sorted(network_dir.rglob("*按压力监测点*坐标匹配*.xlsx"))
    if candidates:
        return candidates[-1]

    raise FileNotFoundError(f"未找到管网结果 Excel：{network_dir}")


def sync_outputs(process_dir: Path, network_dir: Path, data_dir: Path) -> None:
    process_dir = process_dir.resolve()
    network_dir = network_dir.resolve()
    data_dir = data_dir.resolve()

    realtime_src = process_dir / "real-time output"
    if not realtime_src.exists():
        raise FileNotFoundError(f"未找到 real-time output：{realtime_src}")

    network_xlsx = _find_network_xlsx(network_dir)

    backup_dir = data_dir.parent / "data_backup" / datetime.now().strftime("%Y%m%d_%H%M%S")
    if data_dir.exists():
        backup_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(data_dir, backup_dir, ignore=shutil.ignore_patterns(".DS_Store", "__pycache__"))
        print(f"已备份旧 data：{backup_dir}")

    data_dir.mkdir(parents=True, exist_ok=True)
    _copytree_replace(realtime_src, data_dir / "real-time output")
    _copy_history(process_dir, data_dir)
    shutil.copy2(network_xlsx, data_dir / API_NETWORK_XLSX)

    print(f"已同步 real-time output：{realtime_src} -> {data_dir / 'real-time output'}")
    print(f"已同步 report_history：{process_dir / 'report_history'} -> {data_dir / 'history'}")
    print(f"已同步管网 Excel：{network_xlsx} -> {data_dir / API_NETWORK_XLSX}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync existing calculation outputs into API data.")
    parser.add_argument(
        "--process-dir",
        default=os.getenv("PROCESS_WORK_DIR", _default_process_dir()),
        help="包含 real-time output 和 report_history 的中台计算结果目录",
    )
    parser.add_argument(
        "--network-dir",
        default=os.getenv("NETWORK_WORK_DIR", _default_network_dir()),
        help="包含管网计算结果的目录",
    )
    parser.add_argument(
        "--data-dir",
        default=os.getenv("CARBON_API_DATA_DIR", str(PROJECT_DIR / "data")),
        help="FastAPI data 目录",
    )
    args = parser.parse_args()

    sync_outputs(
        process_dir=Path(args.process_dir),
        network_dir=Path(args.network_dir),
        data_dir=Path(args.data_dir),
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"同步已有计算结果失败：{exc}", file=sys.stderr)
        raise
