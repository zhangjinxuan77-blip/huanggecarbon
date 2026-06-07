# -*- coding: utf-8 -*-
"""
管网碳排监测点接口（固定读取年数据）
出参：监测点名称（前端为准）、碳排量、监测时间（最新一条）
"""

import os
import pandas as pd
from typing import List, Dict, Any

from fastapi import APIRouter, HTTPException
from modules.common import format_float_2d

router = APIRouter()

# =============== 路径 ===============
base_dir = os.path.dirname(os.path.dirname(__file__))
XLSX_PATH = os.path.join(base_dir, "data", "管网碳排_按压力监测点_坐标匹配.xlsx")

# =============== 直接固定用“年数据”sheet ===============
YEARLY_SHEET = "Yearly_PressurePoint"


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


def _latest_per_point(df: pd.DataFrame) -> pd.DataFrame:
    """
    每个监测点取最新一条记录
    必须包含列：
    - point（监测点名称）
    - CO2e_kg（碳排量）
    - period 或 ts（时间列）
    """
    if "point" not in df.columns:
        raise HTTPException(status_code=500, detail="数据缺少 point 列")
    if "CO2e_kg" not in df.columns:
        raise HTTPException(status_code=500, detail="数据缺少 CO2e_kg 列")

    time_col = "period" if "period" in df.columns else ("ts" if "ts" in df.columns else None)
    if time_col is None:
        raise HTTPException(status_code=500, detail="数据缺少 period/ts 时间列")

    df = df.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=["point", time_col])

    df = df.sort_values(["point", time_col])
    latest = df.groupby("point", as_index=False).tail(1)
    return latest[["point", time_col, "CO2e_kg"]]


@router.post("/api/network/points-carbon")
def network_points_carbon() -> Dict[str, Any]:
    """
    固定返回年数据，每个监测点最新一条
    出参结构：
    {
      "timeType": "年",
      "sheet": "Yearly_PressurePoint",
      "count": N,
      "data": [{name, carbonKg, monitorTime}]
    }
    """
    df = _load_yearly_sheet()
    latest = _latest_per_point(df)

    items: List[Dict[str, Any]] = []
    time_col = "period" if "period" in df.columns else "ts"

    for _, r in latest.iterrows():
        name = str(r["point"])
        carbon = r["CO2e_kg"]
        t = r[time_col]

        items.append({
            "name": name,
            "carbonKg": 0.0 if pd.isna(carbon) else float(carbon),
            "monitorTime": "" if pd.isna(t) else t.strftime("%Y-%m-%d %H:%M:%S"),
        })

    return format_float_2d({
        "code": 0,
        "msg": "",
        "timeType": "年",
        "sheet": "Yearly_PressurePoint",
        "count": len(items),
        "data": items
    })


@router.post("/api/network/carbon_info")
@router.post("/api/network/carbon-info")
@router.post("/api/network/carbon/info")
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
