# -*- coding: utf-8 -*-
import os
from functools import lru_cache

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from modules.common import format_float_2d


router = APIRouter()

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
SUMMARY_PATH = os.path.join(
    BASE_DIR,
    "data",
    "real-time output",
    "单位水处理强度和减排量",
    "summary_单位水处理强度和减排量.csv",
)

TIME_PERIOD_MAP = {
    1: "latest_24h_hourly",
    2: "latest_7d_daily",
    3: "latest_5w_weekly",
    4: "latest_12m_monthly",
}


class TimeBody(BaseModel):
    timeType: int  # 1=日, 2=周, 3=月, 4=年


@lru_cache(maxsize=1)
def load_summary() -> pd.DataFrame:
    if not os.path.exists(SUMMARY_PATH):
        raise HTTPException(status_code=500, detail=f"summary 文件不存在: {SUMMARY_PATH}")
    try:
        return pd.read_csv(SUMMARY_PATH, encoding="utf-8-sig")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"summary 文件加载失败: {exc}")


def _period(time_type: int) -> str:
    period = TIME_PERIOD_MAP.get(int(time_type))
    if not period:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")
    return period


@router.post("/api/dashboard/overview")
def overview(body: TimeBody):
    df = load_summary()
    period = _period(body.timeType)
    row = df[df["summary周期"].astype(str) == period]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"summary 未找到周期: {period}")

    r = row.iloc[0]
    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "monthTotalTp": float(r["总碳排放_tCO2e"]),
            "proWaterVolume": float(r["水处理量_m3"]),
            "proUnitWater": float(r["单位水处理碳排强度_kgCO2e_per_m3"]),
            "emissionReduction": float(r["光伏减排量_tCO2"]),
        },
    })
