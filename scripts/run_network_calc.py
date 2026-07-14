# -*- coding: utf-8 -*-
"""Run the network carbon Colab script on a server and publish API data."""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
SOURCE_SCRIPT = PROJECT_DIR / "scripts" / "source" / "network_carbon_colab.py"
COLAB_BASE = "/content/drive/MyDrive/管网计算"
COLAB_MATCH_DIR = "/content/drive/MyDrive/管网计算/管网监测点信息匹配"
COLAB_PRESSURE_DIR = "/content/drive/MyDrive/管网计算/管网压力流量区域匹配"
API_NETWORK_XLSX = "管网碳排_按压力监测点_坐标匹配.xlsx"
CALC_NETWORK_XLSX = "管网碳排结果_按压力监测点_坐标匹配.xlsx"


def _clean_notebook_code(code: str) -> str:
    lines = []
    for line in code.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("!"):
            lines.append("# " + line)
        else:
            lines.append(line)
    return "\n".join(lines) + "\n"


def _patched_code(work_dir: Path) -> str:
    code = SOURCE_SCRIPT.read_text(encoding="utf-8")
    code = _clean_notebook_code(code)
    match_dir = work_dir / "管网监测点信息匹配"
    pressure_dir = work_dir / "管网压力流量区域匹配"
    code = code.replace(COLAB_MATCH_DIR, str(match_dir))
    code = code.replace(COLAB_PRESSURE_DIR, str(pressure_dir))
    code = code.replace(COLAB_BASE, str(work_dir))
    return code


def _validate_network_outputs(work_dir: Path) -> Path:
    xlsx = work_dir / CALC_NETWORK_XLSX
    if not xlsx.exists():
        candidates = sorted(work_dir.rglob("*按压力监测点*坐标匹配*.xlsx"))
        if candidates:
            xlsx = candidates[-1]
    if not xlsx.exists():
        raise FileNotFoundError(f"未找到管网碳排结果 Excel：{xlsx}")
    return xlsx


def run_network_calc(work_dir: Path, data_dir: Path, publish: bool) -> None:
    work_dir = work_dir.resolve()
    data_dir = data_dir.resolve()

    if not SOURCE_SCRIPT.exists():
        raise FileNotFoundError(f"未找到源计算脚本：{SOURCE_SCRIPT}")
    work_dir.mkdir(parents=True, exist_ok=True)

    print(f"开始管网碳排计算，工作目录：{work_dir}")
    print("请确认管网原始文件已放入该目录及其子目录。")

    code = _patched_code(work_dir)
    namespace = {
        "__name__": "__main__",
        "__file__": str(SOURCE_SCRIPT),
    }
    exec(compile(code, str(SOURCE_SCRIPT), "exec"), namespace)

    output_xlsx = _validate_network_outputs(work_dir)
    print(f"管网计算结果校验通过：{output_xlsx}")

    if not publish:
        print("未发布到 API data 目录；如需发布请加 --publish")
        return

    data_dir.mkdir(parents=True, exist_ok=True)
    dst = data_dir / API_NETWORK_XLSX
    if dst.exists():
        backup_dir = data_dir.parent / "data_backup" / datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(dst, backup_dir / API_NETWORK_XLSX)
        print(f"已备份旧管网结果：{backup_dir / API_NETWORK_XLSX}")
    shutil.copy2(output_xlsx, dst)
    print(f"已发布管网计算结果到：{dst}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run network carbon calculation.")
    parser.add_argument(
        "--work-dir",
        default=os.getenv("NETWORK_WORK_DIR", "/srv/huanggecarbon/input/network"),
        help="管网计算工作目录，里面放管网原始文件",
    )
    parser.add_argument(
        "--data-dir",
        default=os.getenv("CARBON_API_DATA_DIR", str(PROJECT_DIR / "data")),
        help="FastAPI data 目录",
    )
    parser.add_argument("--publish", action="store_true", help="计算成功后发布到 API data 目录")
    args = parser.parse_args()

    run_network_calc(
        work_dir=Path(args.work_dir),
        data_dir=Path(args.data_dir),
        publish=args.publish,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"管网计算失败：{exc}", file=sys.stderr)
        raise
