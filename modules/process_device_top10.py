# -*- coding: utf-8 -*-
import os
from functools import lru_cache
from typing import Any, Dict, List

import pandas as pd
from fastapi import APIRouter, HTTPException

from modules.common import format_float_2d


router = APIRouter()

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
TOP10_PATH = os.path.join(
    BASE_DIR,
    "data",
    "real-time output",
    "process_stage_outputs",
    "工艺单元电耗Top10",
    "summary_daily_工艺单元电耗Top10.csv",
)


@lru_cache(maxsize=1)
def load_top10() -> List[Dict[str, Any]]:
    if not os.path.exists(TOP10_PATH):
        raise HTTPException(status_code=500, detail=f"Top10 文件不存在: {TOP10_PATH}")

    try:
        df = pd.read_csv(TOP10_PATH, encoding="utf-8-sig")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Top10 文件加载失败: {exc}")

    required_cols = ["rank", "process_unit", "electric_carbon_kg_sum"]
    for col in required_cols:
        if col not in df.columns:
            raise HTTPException(status_code=500, detail=f"Top10 文件缺少字段: {col}")

    df = df.copy()
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce").fillna(9999)
    df["electric_carbon_kg_sum"] = pd.to_numeric(
        df["electric_carbon_kg_sum"],
        errors="coerce",
    ).fillna(0.0)
    df = df.sort_values("rank").head(10)

    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        name = str(row["process_unit"]).strip()
        if not name or name == "nan":
            continue
        records.append({
            "name": name,
            "value": float(row["electric_carbon_kg_sum"]),
        })

    return records


@router.get("/api/process/device_top10")
def device_top10():
    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": load_top10(),
    })
