# -*- coding: utf-8 -*-
"""
碳排趋势接口
接口：POST /api/dashboard/carbon_trend
兼容：POST /api/dashboard/trend
入参：{"timeType":1}，1=日, 2=周, 3=月, 4=年
出参：按腾讯文档格式返回趋势数组，数值口径为趋势 CSV 中的实际碳排放量
"""

import os
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from modules.common import format_float_2d


router = APIRouter()


class TimeBody(BaseModel):
    timeType: int


BASE_DIR = os.path.dirname(os.path.dirname(__file__))
TREND_DIR = os.path.join(BASE_DIR, "data", "real-time output", "scope123_总汇总")
UNIT = "kgCO2e"

TIME_CONFIG = {
    1: {"period": "日", "file": "latest_24h_hourly.csv"},
    2: {"period": "周", "file": "latest_7d_daily.csv"},
    3: {"period": "月", "file": "latest_5w_weekly.csv"},
    4: {"period": "年", "file": "latest_12m_monthly.csv"},
}

REQUIRED_COLUMNS = [
    "period_start",
    "scope1_carbon_kg",
    "scope2_carbon_kg",
    "scope3_carbon_kg",
    "total_carbon_kg",
]


def _read_trend_file(filename: str) -> pd.DataFrame:
    path = os.path.join(TREND_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=500, detail=f"未找到数据文件：{path}")

    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"CSV 读取失败：{path}，{exc}")

    if df.empty:
        raise HTTPException(status_code=404, detail=f"CSV 没有数据：{path}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise HTTPException(status_code=500, detail=f"CSV 缺少字段：{missing}")

    df = df.copy()
    df["period_start"] = pd.to_datetime(df["period_start"], errors="coerce")
    df = df.sort_values("period_start")
    return df


def _format_time(value: Any, period: str) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return str(value)

    if period == "日":
        return dt.strftime("%m-%d %H:%M")
    if period in ("周", "月"):
        return dt.strftime("%m-%d")
    if period == "年":
        return f"{dt.month}月"
    return str(value)


def build_payload(time_type: int) -> dict:
    config = TIME_CONFIG.get(time_type)
    if not config:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    df = _read_trend_file(config["file"])
    source = []
    x_axis = []
    total_data = []
    scope1_data = []
    scope2_data = []
    scope3_data = []
    for _, row in df.iterrows():
        time_label = _format_time(row["period_start"], config["period"])
        total_value = float(row["total_carbon_kg"])
        scope1_value = float(row["scope1_carbon_kg"])
        scope2_value = float(row["scope2_carbon_kg"])
        scope3_value = float(row["scope3_carbon_kg"])
        x_axis.append(time_label)
        total_data.append(total_value)
        scope1_data.append(scope1_value)
        scope2_data.append(scope2_value)
        scope3_data.append(scope3_value)
        source.append({
            "时间": time_label,
            "总碳排放量": total_value,
            "范围1": scope1_value,
            "范围2": scope2_value,
            "范围3": scope3_value,
        })

    dimensions = ["时间", "总碳排放量", "范围1", "范围2", "范围3"]
    return {
        "code": 0,
        "msg": "",
        "data": {
            "unit": UNIT,
            "period": config["period"],
            "xAxis": x_axis,
            "data": total_data,
            "series": [
                {
                    "name": "总碳排放量",
                    "type": "line",
                    "unit": UNIT,
                    "data": total_data,
                },
                {
                    "name": "范围1",
                    "type": "line",
                    "unit": UNIT,
                    "data": scope1_data,
                },
                {
                    "name": "范围2",
                    "type": "line",
                    "unit": UNIT,
                    "data": scope2_data,
                },
                {
                    "name": "范围3",
                    "type": "line",
                    "unit": UNIT,
                    "data": scope3_data,
                },
            ],
            "dimensions": dimensions,
            "source": source,
            "dimensionsMapping": dimensions,
        },
    }


@router.post("/api/dashboard/carbon_trend")
@router.post("/api/dashboard/trend")
def carbon_trend(body: TimeBody):
    return format_float_2d(build_payload(int(body.timeType)))
