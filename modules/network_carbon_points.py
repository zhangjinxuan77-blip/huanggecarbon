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

# =============== 直接固定用“年数据”sheet ===============
YEARLY_SHEET = "Yearly_PressurePoint"
HOURLY_SHEET = "Hourly_PressurePoint"


# 简单缓存（避免重复读 Excel）
_CACHE: Dict[str, pd.DataFrame] = {}


def _load_yearly_sheet() -> pd.DataFrame:
    if YEARLY_SHEET in _CACHE:
        return _CACHE[YEARLY_SHEET]

    if not os.path.exists(XLSX_PATH):
        return pd.DataFrame(columns=["point", "period", "CO2e_kg"])

    try:
        df = pd.read_excel(XLSX_PATH, sheet_name=YEARLY_SHEET)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取 Yearly_PressurePoint 失败: {repr(e)}")

    _CACHE[YEARLY_SHEET] = df
    return df


def _load_hourly_sheet() -> pd.DataFrame:
    if HOURLY_SHEET in _CACHE:
        return _CACHE[HOURLY_SHEET]

    if not os.path.exists(XLSX_PATH):
        return pd.DataFrame(columns=["ts", "point", "pressure_m", "flow_m3_h", "CO2e_kg"])

    try:
        df = pd.read_excel(XLSX_PATH, sheet_name=HOURLY_SHEET)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取 Hourly_PressurePoint 失败: {repr(e)}")

    _CACHE[HOURLY_SHEET] = df
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


def _latest_yearly_rows() -> pd.DataFrame:
    df = _load_yearly_sheet()
    required = ["period", "CO2e_kg", "flow_m3_h", "kWh", "SE_kWh_m3"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise HTTPException(status_code=500, detail=f"数据缺少字段: {missing}")

    df = df.copy()
    df["period"] = pd.to_datetime(df["period"], errors="coerce")
    df = df.dropna(subset=["period"])
    if df.empty:
        return df

    latest_period = df["period"].max()
    latest = df[df["period"] == latest_period].copy()
    for col in ["CO2e_kg", "flow_m3_h", "kWh", "SE_kWh_m3"]:
        latest[col] = pd.to_numeric(latest[col], errors="coerce").fillna(0.0)
    return latest


def _fmt_2d(value: float) -> str:
    return f"{float(value):.2f}"


def _fmt_6d(value: float) -> str:
    return f"{float(value):.6f}"


def _fmt_marker_num(value: Any) -> str:
    if pd.isna(value):
        return "0"
    return f"{float(value):.2f}"


@router.post("/api/network/carbon_info")
def network_carbon_info() -> Dict[str, Any]:
    rows = _latest_yearly_rows()
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
            "totalCe": _fmt_2d(total_ce),
            "avgCe": _fmt_2d(avg_ce),
            "avgWtf": _fmt_2d(avg_wtf),
            "unitECWaterTrans": _fmt_6d(unit_energy),
        },
    }


@router.post("/api/network/map")
def network_carbon_map() -> Dict[str, Any]:
    hourly = _load_hourly_sheet()
    coords = _load_coord_sheet()

    hourly_required = ["ts", "point", "pressure_m", "flow_m3_h", "CO2e_kg"]
    coord_required = ["监测点名称", "经度", "纬度"]
    missing_hourly = [c for c in hourly_required if c not in hourly.columns]
    missing_coord = [c for c in coord_required if c not in coords.columns]
    if missing_hourly:
        raise HTTPException(status_code=500, detail=f"小时数据缺少字段: {missing_hourly}")
    if missing_coord:
        raise HTTPException(status_code=500, detail=f"经纬度数据缺少字段: {missing_coord}")

    hourly = hourly.copy()
    hourly["ts"] = pd.to_datetime(hourly["ts"], errors="coerce")
    hourly["CO2e_kg"] = pd.to_numeric(hourly["CO2e_kg"], errors="coerce")
    hourly = hourly.dropna(subset=["ts", "point", "CO2e_kg"])
    hourly = hourly[hourly["CO2e_kg"] > 0].copy()
    hourly = hourly.sort_values(["point", "ts"]).groupby("point", as_index=False).tail(1)
    hourly["point_key"] = hourly["point"].map(_norm_point)
    for col in ["pressure_m", "flow_m3_h"]:
        hourly[col] = pd.to_numeric(hourly[col], errors="coerce").fillna(0.0)

    coords = coords.copy()
    coords["point_key"] = coords["监测点名称"].map(_norm_point)
    coords["经度"] = pd.to_numeric(coords["经度"], errors="coerce")
    coords["纬度"] = pd.to_numeric(coords["纬度"], errors="coerce")
    coords = coords.dropna(subset=["point_key", "经度", "纬度"])

    merged = hourly.merge(
        coords[["point_key", "监测点名称", "经度", "纬度"]],
        on="point_key",
        how="inner",
    )

    markers = []
    for _, row in merged.iterrows():
        markers.append({
            "position": [float(row["经度"]), float(row["纬度"])],
            "title": str(row["监测点名称"]),
            "pressure": _fmt_marker_num(row["pressure_m"]),
            "flow": _fmt_marker_num(row["flow_m3_h"]),
            "carbonEmission": _fmt_marker_num(row["CO2e_kg"]),
            "uploadTime": row["ts"].strftime("%Y-%m-%d %H:%M:%S"),
        })

    return {
        "code": 0,
        "msg": "",
        "data": {
            "markers": markers,
        },
    }
