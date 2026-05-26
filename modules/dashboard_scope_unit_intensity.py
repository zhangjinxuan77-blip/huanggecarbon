# -*- coding: utf-8 -*-
import os
from functools import lru_cache

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from modules.common import format_float_2d


router = APIRouter()

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", "real-time output", "单位水处理强度和减排量")

TREND_FILE_MAP = {
    1: "单位水处理强度_日趋势.csv",
    2: "单位水处理强度_周趋势.csv",
    3: "单位水处理强度_月趋势.csv",
    4: "单位水处理强度_月趋势.csv",
}

TIME_COLUMN_MAP = {
    1: "scope日期",
    2: "scope周日期",
    3: "scope月份",
    4: "scope月份",
}


class TimeBody(BaseModel):
    timeType: int  # 1=日, 2=周, 3=月, 4=年


@lru_cache(maxsize=4)
def load_trend(filename: str) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=500, detail=f"趋势文件不存在: {path}")
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"趋势文件加载失败: {exc}")


def _format_time(value, time_type: int) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return str(value)
    if time_type in (3, 4):
        return f"{ts.month}月"
    return ts.strftime("%m-%d")


@router.post("/api/dashboard/unit_intensity")
def unit_intensity(body: TimeBody):
    time_type = int(body.timeType)
    filename = TREND_FILE_MAP.get(time_type)
    time_col = TIME_COLUMN_MAP.get(time_type)
    if not filename or not time_col:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    df = load_trend(filename)
    required_cols = [time_col, "水处理量_m3", "单位水处理碳排强度_kgCO2e_per_m3"]
    for col in required_cols:
        if col not in df.columns:
            raise HTTPException(status_code=500, detail=f"趋势文件缺少字段: {col}")

    source = []
    for _, row in df.iterrows():
        source.append({
            "时间": _format_time(row[time_col], time_type),
            "总处理水量": float(row["水处理量_m3"]),
            "单位处理强度": float(row["单位水处理碳排强度_kgCO2e_per_m3"]),
        })

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "dimensions": ["时间", "总处理水量", "单位处理强度"],
            "source": source,
            "dimensionsMapping": ["时间", "总处理水量", "单位处理强度"],
        },
    })
