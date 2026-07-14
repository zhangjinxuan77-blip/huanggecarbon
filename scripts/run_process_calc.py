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
import tarfile
from datetime import datetime
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
SOURCE_SCRIPT = PROJECT_DIR / "scripts" / "source" / "process_carbon_colab.py"
COLAB_BASE = "/content/drive/MyDrive/中台一年历史数据"


def _default_process_input() -> str:
    candidates = [
        PROJECT_DIR.parent / "Development" / "20260511.tar.gz",
        PROJECT_DIR.parent / "Development" / "20260511.csv",
        PROJECT_DIR.parent / "中台一年历史数据" / "20260511.tar.gz",
        PROJECT_DIR.parent / "中台一年历史数据" / "20260511.csv",
    ]
    for local_input in candidates:
        if local_input.exists():
            return str(local_input)
    return "/srv/huanggecarbon/input/process/20260511.csv"


def _validate_process_input(input_file: Path) -> None:
    """Validate CSV input or a single-CSV tar archive readable by pandas."""
    if not input_file.exists():
        raise FileNotFoundError(f"未找到工艺段原始数据：{input_file}")
    if not str(input_file).lower().endswith((".tar.gz", ".tgz", ".tar")):
        return
    with tarfile.open(input_file, "r:*") as archive:
        csv_members = [
            member for member in archive.getmembers()
            if member.isfile() and member.name.lower().endswith(".csv")
        ]
    if len(csv_members) != 1:
        raise ValueError(
            f"压缩包必须且只能包含一个 CSV，当前发现 {len(csv_members)} 个：{input_file}"
        )


def _default_process_work_dir() -> str:
    local_work_dir = PROJECT_DIR.parent / "中台一年历史数据"
    if local_work_dir.exists():
        return str(local_work_dir)
    return "/srv/huanggecarbon/work/process_latest"


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


def _patched_code(input_file: Path, work_dir: Path, reuse_extracted: bool = False) -> str:
    code = SOURCE_SCRIPT.read_text(encoding="utf-8")
    # The exported notebook in the repository contains a duplicated full copy.
    # Execute only the first copy so the same input is never calculated twice.
    notebook_start = '"""实时数据整理拆分"""'
    first_start = code.find(notebook_start)
    second_start = code.find(notebook_start, first_start + len(notebook_start))
    if first_start >= 0 and second_start >= 0:
        history_marker = "# 报告历史数据导出"
        colab_upload_marker = "from google.colab import userdata"
        history_start = code.find(history_marker, second_start)
        history_end = code.find(colab_upload_marker, history_start)
        history_tail = (
            code[history_start:history_end]
            if history_start >= 0 and history_end > history_start
            else ""
        )
        code = code[:second_start] + "\n" + history_tail
        print("检测到重复 Notebook 内容，已保留首份计算流程及唯一历史导出段")
    if reuse_extracted:
        parquet_dir = work_dir / "碳排放核算"
        parquet_files = list(parquet_dir.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"没有可复用的 Parquet 中间文件：{parquet_dir}")
        marker = '"""碳排计算"""'
        marker_pos = code.find(marker)
        if marker_pos < 0:
            raise RuntimeError("源计算脚本中未找到碳排计算阶段标记")
        code = code[marker_pos:]
        print(f"复用 {len(parquet_files)} 个 Parquet 中间文件，跳过原始 CSV 扫描")
    code = code.replace(
        'input_file = "/content/drive/MyDrive/中台一年历史数据/20260511.csv"',
        f'input_file = r"{input_file}"',
    )
    # 注入到源脚本的普通字符串中时统一使用正斜杠，避免Windows路径中的
    # \n、\t等片段被Python解释成换行或制表符。
    code = code.replace(COLAB_BASE, work_dir.as_posix())
    return code


def run_process_calc(
    input_file: Path,
    work_dir: Path,
    data_dir: Path,
    publish: bool,
    reuse_extracted: bool = False,
) -> None:
    input_file = input_file.resolve()
    work_dir = work_dir.resolve()
    data_dir = data_dir.resolve()

    _validate_process_input(input_file)
    if not SOURCE_SCRIPT.exists():
        raise FileNotFoundError(f"未找到源计算脚本：{SOURCE_SCRIPT}")

    work_dir.mkdir(parents=True, exist_ok=True)
    # The original notebook creates many leaf folders with mkdir(exist_ok=True)
    # and assumes these Colab parent folders already exist.
    for parent in ["Outputs", "real-time output", "report_history", "碳排放核算"]:
        (work_dir / parent).mkdir(parents=True, exist_ok=True)
    print(f"开始工艺段碳排计算：{input_file}")
    print(f"计算工作目录：{work_dir}")

    code = _patched_code(
        input_file=input_file,
        work_dir=work_dir,
        reuse_extracted=reuse_extracted,
    )
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
        default=os.getenv("PROCESS_INPUT_FILE", _default_process_input()),
        help="工艺段原始测点 CSV，或仅包含一个 CSV 的 tar/tar.gz 压缩包",
    )
    parser.add_argument(
        "--work-dir",
        default=os.getenv("PROCESS_WORK_DIR", _default_process_work_dir()),
        help="计算工作目录",
    )
    parser.add_argument(
        "--data-dir",
        default=os.getenv("CARBON_API_DATA_DIR", str(PROJECT_DIR / "data")),
        help="FastAPI data 目录",
    )
    parser.add_argument("--publish", action="store_true", help="计算成功后发布到 API data 目录")
    parser.add_argument(
        "--reuse-extracted",
        action="store_true",
        help="复用工作目录中已生成的 Parquet，跳过原始 CSV/压缩包扫描",
    )
    args = parser.parse_args()

    run_process_calc(
        input_file=Path(args.input),
        work_dir=Path(args.work_dir),
        data_dir=Path(args.data_dir),
        publish=args.publish,
        reuse_extracted=args.reuse_extracted,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"工艺段计算失败：{exc}", file=sys.stderr)
        raise
