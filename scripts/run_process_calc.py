# -*- coding: utf-8 -*-
"""Run the process carbon Colab script on a server and publish API data.

The original calculation file is kept in scripts/source/process_carbon_colab.py.
This wrapper only replaces Colab paths with server paths and copies the generated
outputs into the FastAPI data directory after a successful run.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
SOURCE_SCRIPT = PROJECT_DIR / "scripts" / "source" / "process_carbon_colab.py"
COLAB_BASE = "/content/drive/MyDrive/中台一年历史数据"


def _copytree_replace(src: Path, dst: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(f"未找到计算结果目录：{src}")
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def _copy_report_history(work_dir: Path, data_dir: Path) -> None:
    history_src = work_dir / "report_history"
    if not history_src.exists():
        raise FileNotFoundError(f"未找到报告历史目录：{history_src}")

    history_dst = data_dir / "history"
    history_dst.mkdir(parents=True, exist_ok=True)
    for filename in [
        "scope123_daily.csv",
        "process_stage_daily.csv",
        "process_unit_daily.csv",
    ]:
        src = history_src / filename
        if not src.exists():
            raise FileNotFoundError(f"未找到报告历史文件：{src}")
        shutil.copy2(src, history_dst / filename)


def _validate_process_outputs(work_dir: Path) -> None:
    required = [
        work_dir / "real-time output" / "scope123_总汇总" / "summary.csv",
        work_dir / "real-time output" / "scope123_总汇总" / "latest_24h_hourly.csv",
        work_dir / "real-time output" / "scope123_总汇总" / "latest_7d_daily.csv",
        work_dir / "real-time output" / "process_stage_outputs" / "工艺段汇总" / "summary.csv",
        work_dir / "report_history" / "scope123_daily.csv",
        work_dir / "report_history" / "process_stage_daily.csv",
        work_dir / "report_history" / "process_unit_daily.csv",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError("工艺段计算结果不完整：\n" + "\n".join(missing))


def _patched_code(input_file: Path, work_dir: Path) -> str:
    code = SOURCE_SCRIPT.read_text(encoding="utf-8")
    code = code.replace(
        'input_file = "/content/drive/MyDrive/中台一年历史数据/20260511.csv"',
        f'input_file = r"{input_file}"',
    )
    code = code.replace(COLAB_BASE, str(work_dir))
    return code


def run_process_calc(input_file: Path, work_dir: Path, data_dir: Path, publish: bool) -> None:
    input_file = input_file.resolve()
    work_dir = work_dir.resolve()
    data_dir = data_dir.resolve()

    if not input_file.exists():
        raise FileNotFoundError(f"未找到工艺段原始 CSV：{input_file}")
    if not SOURCE_SCRIPT.exists():
        raise FileNotFoundError(f"未找到源计算脚本：{SOURCE_SCRIPT}")

    work_dir.mkdir(parents=True, exist_ok=True)
    print(f"开始工艺段碳排计算：{input_file}")
    print(f"计算工作目录：{work_dir}")

    code = _patched_code(input_file=input_file, work_dir=work_dir)
    namespace = {
        "__name__": "__main__",
        "__file__": str(SOURCE_SCRIPT),
    }
    exec(compile(code, str(SOURCE_SCRIPT), "exec"), namespace)

    _validate_process_outputs(work_dir)
    print("工艺段计算结果校验通过")

    if not publish:
        print("未发布到 API data 目录；如需发布请加 --publish")
        return

    data_dir.mkdir(parents=True, exist_ok=True)
    backup_dir = data_dir.parent / "data_backup" / datetime.now().strftime("%Y%m%d_%H%M%S")
    if data_dir.exists():
        backup_dir.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(data_dir, backup_dir)
        print(f"已备份旧 data：{backup_dir}")

    _copytree_replace(work_dir / "real-time output", data_dir / "real-time output")
    _copy_report_history(work_dir, data_dir)
    print(f"已发布工艺段计算结果到：{data_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run process carbon calculation.")
    parser.add_argument(
        "--input",
        default=os.getenv("PROCESS_INPUT_FILE", "/srv/huanggecarbon/input/process/20260511.csv"),
        help="工艺段原始测点 CSV 文件",
    )
    parser.add_argument(
        "--work-dir",
        default=os.getenv("PROCESS_WORK_DIR", "/srv/huanggecarbon/work/process_latest"),
        help="计算工作目录",
    )
    parser.add_argument(
        "--data-dir",
        default=os.getenv("CARBON_API_DATA_DIR", str(PROJECT_DIR / "data")),
        help="FastAPI data 目录",
    )
    parser.add_argument("--publish", action="store_true", help="计算成功后发布到 API data 目录")
    args = parser.parse_args()

    run_process_calc(
        input_file=Path(args.input),
        work_dir=Path(args.work_dir),
        data_dir=Path(args.data_dir),
        publish=args.publish,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"工艺段计算失败：{exc}", file=sys.stderr)
        raise
