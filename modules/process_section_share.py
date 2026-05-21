# -*- coding: utf-8 -*-
"""
工艺段碳排放结构占比
接口：POST /api/process/section_share
入参：{"timeType":1}，1=日, 2=周, 3=月, 4=年
数据源：data/real-time output/process_stage_outputs/工艺段汇总/summary.csv
"""

import os

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from modules.common import format_float_2d


router = APIRouter()


class TimeBody(BaseModel):
    timeType: int


APP_DIR = os.path.dirname(os.path.dirname(__file__))
SUMMARY_PATH = os.path.join(
    APP_DIR,
    "data",
    "real-time output",
    "process_stage_outputs",
    "工艺段汇总",
    "summary.csv",
)

TIME_CONFIG = {
    1: "latest_24h_hourly",
    2: "latest_7d_daily",
    3: "latest_5w_weekly",
    4: "latest_12m_monthly",
}

STAGE_LABELS = {
    "01_原水取水段": "原水取水",
    "02_供水段": "供水",
    "03_预处理": "预处理",
    "04_混凝沉淀": "混凝沉淀",
    "05_过滤": "过滤",
    "06_深度处理": "深度处理",
    "07_污泥处理": "污泥处理",
}

STAGE_ORDER = list(STAGE_LABELS.keys())


def _load_summary() -> pd.DataFrame:
    if not os.path.exists(SUMMARY_PATH):
        raise HTTPException(status_code=500, detail=f"文件不存在：{SUMMARY_PATH}")

    try:
        df = pd.read_csv(SUMMARY_PATH, encoding="utf-8-sig")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"CSV 读取失败：{SUMMARY_PATH}，{exc}")

    required = ["summary_period", "summary_level", "process_stage", "stage_share_of_plant_avg"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise HTTPException(status_code=500, detail=f"CSV 缺少字段：{missing}")

    df["stage_share_of_plant_avg"] = pd.to_numeric(
        df["stage_share_of_plant_avg"],
        errors="coerce",
    ).fillna(0.0)
    return df


def _source(time_type: int) -> list[dict]:
    summary_period = TIME_CONFIG.get(time_type)
    if not summary_period:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    df = _load_summary()
    rows = df[
        (df["summary_period"] == summary_period)
        & (df["summary_level"] == "detail")
    ].copy()

    source = []
    for stage in STAGE_ORDER:
        row = rows[rows["process_stage"] == stage]
        value = float(row["stage_share_of_plant_avg"].iloc[0]) * 100.0 if not row.empty else 0.0
        source.append({"name": STAGE_LABELS[stage], "data": value})
    return source


@router.post("/api/process/section_share")
def process_section_share(body: TimeBody):
    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "unit": "%",
            "dimensions": ["name", "data"],
            "source": _source(int(body.timeType)),
            "dimensionsMapping": ["name", "data"],
        },
    })
