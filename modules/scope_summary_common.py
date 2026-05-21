# -*- coding: utf-8 -*-
import os
from functools import lru_cache
from typing import Any, Optional

import pandas as pd
from fastapi import HTTPException


BASE_DIR = os.path.dirname(os.path.dirname(__file__))
REALTIME_DIR = os.path.join(BASE_DIR, "data", "real-time output")

TIME_PERIOD_MAP = {
    1: "latest_24h_hourly",
    2: "latest_7d_daily",
    3: "latest_5w_weekly",
    4: "latest_12m_monthly",
}


def validate_time_type(time_type: int) -> str:
    period = TIME_PERIOD_MAP.get(int(time_type))
    if not period:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")
    return period


@lru_cache(maxsize=16)
def load_summary(relative_dir: str) -> pd.DataFrame:
    path = os.path.join(REALTIME_DIR, relative_dir, "summary.csv")
    if not os.path.exists(path):
        raise HTTPException(status_code=500, detail=f"summary.csv 不存在: {path}")
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"summary.csv 加载失败: {exc}")


def summary_row(relative_dir: str, time_type: int, level: Optional[str] = None) -> pd.Series:
    period = validate_time_type(time_type)
    df = load_summary(relative_dir)
    sub = df[df["summary_period"].astype(str) == period]
    if level is not None and "summary_level" in sub.columns:
        sub = sub[sub["summary_level"].astype(str) == level]
    if sub.empty:
        raise HTTPException(status_code=404, detail=f"summary.csv 未找到周期: {period}")
    return sub.iloc[0]


def summary_rows(relative_dir: str, time_type: int, level: Optional[str] = None) -> pd.DataFrame:
    period = validate_time_type(time_type)
    df = load_summary(relative_dir)
    sub = df[df["summary_period"].astype(str) == period]
    if level is not None and "summary_level" in sub.columns:
        sub = sub[sub["summary_level"].astype(str) == level]
    if sub.empty:
        raise HTTPException(status_code=404, detail=f"summary.csv 未找到周期: {period}")
    return sub.copy()


def f2(value: Any) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return round(float(value), 2)
    except Exception:
        return 0.0


def pct2(value: Any) -> float:
    return f2(float(value or 0) * 100)
