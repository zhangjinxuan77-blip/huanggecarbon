# -*- coding: utf-8 -*-
"""
管网碳排接口：
- POST /api/network/carbon_info
- POST /api/network/map
"""

import os
import re
import pandas as pd
from typing import Dict, Any

from fastapi import APIRouter, HTTPException

router = APIRouter()

# =============== 路径 ===============
base_dir = os.path.dirname(os.path.dirname(__file__))
XLSX_PATH = os.path.join(base_dir, "data", "管网碳排_按压力监测点_坐标匹配.xlsx")
COORD_XLSX_PATH = os.path.join(base_dir, "data", "监测点经纬度.xlsx")

DAILY_SHEET = "Daily_PressurePoint"


# 简单缓存（避免重复读 Excel）
_CACHE: Dict[str, pd.DataFrame] = {}


def _load_daily_sheet() -> pd.DataFrame:
    if DAILY_SHEET in _CACHE:
        return _CACHE[DAILY_SHEET]

    if not os.path.exists(XLSX_PATH):
        return pd.DataFrame(
            columns=["period", "point", "pressure_m", "flow_m3_h", "kWh", "CO2e_kg", "SE_kWh_m3"]
        )

    try:
        df = pd.read_excel(XLSX_PATH, sheet_name=DAILY_SHEET)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取 Daily_PressurePoint 失败: {repr(e)}")

    _CACHE[DAILY_SHEET] = df
    return df


def _load_coord_sheet() -> pd.DataFrame:
    cache_key = "coord_points"
    if cache_key in _CACHE:
        return _CACHE[cache_key]

    if not os.path.exists(COORD_XLSX_PATH):
        raise HTTPException(status_code=500, detail=f"经纬度文件不存在: {COORD_XLSX_PATH}")

    try:
        df = pd.read_excel(COORD_XLSX_PATH)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取监测点经纬度失败: {repr(e)}")

    _CACHE[cache_key] = df
    return df


def _norm_point(value: Any) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[（(]hd[）)]$", "", text)
    text = re.sub(r"hd$", "", text)
    return text


def _latest_daily_rows() -> pd.DataFrame:
    df = _load_daily_sheet()
    required = ["period", "point", "pressure_m", "CO2e_kg", "flow_m3_h", "kWh", "SE_kWh_m3"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise HTTPException(status_code=500, detail=f"数据缺少字段: {missing}")

    df = df.copy()
    df["period"] = pd.to_datetime(df["period"], errors="coerce")
    for col in ["pressure_m", "CO2e_kg", "flow_m3_h", "kWh", "SE_kWh_m3"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["period", "point"])
    if df.empty:
        return df

    positive_periods = df.loc[df["CO2e_kg"].fillna(0.0) > 0, "period"]
    if positive_periods.empty:
        return df.iloc[0:0].copy()

    latest_period = positive_periods.max()
    latest = df[df["period"] == latest_period].copy()
    for col in ["pressure_m", "CO2e_kg", "flow_m3_h", "kWh", "SE_kWh_m3"]:
        latest[col] = latest[col].fillna(0.0)
    return latest


def _fmt_2d(value: float, unit: str) -> str:
    return f"{float(value):.2f} {unit}"


def _fmt_6d(value: float, unit: str) -> str:
    return f"{float(value):.6f} {unit}"


def _fmt_marker_num(value: Any, unit: str) -> str:
    if pd.isna(value):
        return f"0.00 {unit}"
    return f"{float(value):.2f} {unit}"


@router.post("/api/network/carbon_info")
def network_carbon_info() -> Dict[str, Any]:
    rows = _latest_daily_rows()
    if rows.empty:
        total_ce = avg_ce = avg_wtf = unit_energy = 0.0
    else:
        total_ce = float(rows["CO2e_kg"].sum())
        avg_ce = float(rows["CO2e_kg"].mean())
        avg_wtf = float(rows["flow_m3_h"].mean())
        total_flow = float(rows["flow_m3_h"].sum())
        total_kwh = float(rows["kWh"].sum())
        unit_energy = total_kwh / total_flow if total_flow else float(rows["SE_kWh_m3"].mean())

    return {
        "code": 0,
        "msg": "",
        "data": {
            "totalCe": _fmt_2d(total_ce, "kgCO2e"),
            "avgCe": _fmt_2d(avg_ce, "kgCO2e"),
            "avgWtf": _fmt_2d(avg_wtf, "m3/h"),
            "unitECWaterTrans": _fmt_6d(unit_energy, "kWh/m3"),
        },
    }


@router.post("/api/network/map")
def network_carbon_map() -> Dict[str, Any]:
    daily = _latest_daily_rows()
    daily = daily[daily["CO2e_kg"] > 0].copy()
    coords = _load_coord_sheet()

    coord_required = ["监测点名称", "经度", "纬度"]
    missing_coord = [c for c in coord_required if c not in coords.columns]
    if missing_coord:
        raise HTTPException(status_code=500, detail=f"经纬度数据缺少字段: {missing_coord}")

    daily = daily.copy()
    daily["point_key"] = daily["point"].map(_norm_point)

    coords = coords.copy()
    coords["point_key"] = coords["监测点名称"].map(_norm_point)
    coords["经度"] = pd.to_numeric(coords["经度"], errors="coerce")
    coords["纬度"] = pd.to_numeric(coords["纬度"], errors="coerce")
    coords = coords.dropna(subset=["point_key", "经度", "纬度"])

    merged = daily.merge(
        coords[["point_key", "监测点名称", "经度", "纬度"]],
        on="point_key",
        how="inner",
    )

    markers = []
    for _, row in merged.iterrows():
        markers.append({
            "position": [float(row["经度"]), float(row["纬度"])],
            "title": str(row["监测点名称"]),
            "pressure": _fmt_marker_num(row["pressure_m"], "m"),
            "flow": _fmt_marker_num(row["flow_m3_h"], "m3/h"),
            "carbonEmission": _fmt_marker_num(row["CO2e_kg"], "kgCO2e"),
            "uploadTime": row["period"].strftime("%Y-%m-%d %H:%M:%S"),
        })

    return {
        "code": 0,
        "msg": "",
        "data": {
            "markers": markers,
        },
    }
