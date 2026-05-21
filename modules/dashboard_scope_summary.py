# -*- coding: utf-8 -*-
"""
碳排数据汇总接口
接口：POST /api/dashboard/scope_summary
入参：{"timeType":1}，1=日, 2=周, 3=月, 4=年
出参：从 summary.csv 返回单组范围汇总数据，单位 kgCO2e
"""

import os

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .common import format_float_2d


router = APIRouter()


class TimeBody(BaseModel):
    timeType: int


BASE_DIR = os.path.dirname(os.path.dirname(__file__))
SUMMARY_PATH = os.path.join(
    BASE_DIR,
    "data",
    "real-time output",
    "scope123_总汇总",
    "summary.csv",
)
UNIT = "kgCO2e"

TIME_CONFIG = {
    1: {"period": "日", "summary_period": "latest_24h_hourly"},
    2: {"period": "周", "summary_period": "latest_7d_daily"},
    3: {"period": "月", "summary_period": "latest_5w_weekly"},
    4: {"period": "年", "summary_period": "latest_12m_monthly"},
}

REQUIRED_COLUMNS = [
    "summary_period",
    "scope1_carbon_kg_sum",
    "scope2_carbon_kg_sum",
    "scope3_carbon_kg_sum",
    "total_carbon_kg_sum",
]


def _summary_table() -> pd.DataFrame:
    if not os.path.exists(SUMMARY_PATH):
        raise HTTPException(status_code=500, detail=f"未找到数据文件：{SUMMARY_PATH}")

    try:
        df = pd.read_csv(SUMMARY_PATH, encoding="utf-8-sig")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"CSV 读取失败：{SUMMARY_PATH}，{exc}")

    if df.empty:
        raise HTTPException(status_code=404, detail=f"CSV 没有数据：{SUMMARY_PATH}")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise HTTPException(status_code=500, detail=f"CSV 缺少字段：{missing}")

    return df


def _summary_row(time_type: int) -> tuple[str, pd.Series]:
    config = TIME_CONFIG.get(time_type)
    if not config:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    df = _summary_table()
    rows = df[df["summary_period"] == config["summary_period"]]
    if rows.empty:
        raise HTTPException(status_code=404, detail=f"summary.csv 未找到：{config['summary_period']}")

    return config["period"], rows.iloc[0]


def build_payload(time_type: int) -> dict:
    period, row = _summary_row(time_type)

    scope1 = float(row["scope1_carbon_kg_sum"])
    scope2 = float(row["scope2_carbon_kg_sum"])
    scope3 = float(row["scope3_carbon_kg_sum"])
    total = float(row["total_carbon_kg_sum"])

    return {
        "code": 0,
        "msg": "",
        "data": {
            "unit": UNIT,
            "period": period,
            "dimensions": ["name", "data"],
            "source": [
                {"name": "范围1", "data": scope1, "dataWithUnit": f"{scope1:.2f} {UNIT}"},
                {"name": "范围2", "data": scope2, "dataWithUnit": f"{scope2:.2f} {UNIT}"},
                {"name": "范围3", "data": scope3, "dataWithUnit": f"{scope3:.2f} {UNIT}"},
            ],
            "totalCarbonEmission": total,
            "totalCarbonEmissionWithUnit": f"{total:.2f} {UNIT}",
            "dimensionsMapping": ["name", "data"],
        },
    }


@router.post("/api/dashboard/scope_summary")
def scope_summary(body: TimeBody):
    return format_float_2d(build_payload(int(body.timeType)))
