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
        raise HTTPException(status_code=404, detail="未找到管网碳排 Excel 文件")

    try:
        df = pd.read_excel(XLSX_PATH, sheet_name=YEARLY_SHEET)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取 Yearly_PressurePoint 失败: {repr(e)}")

    _CACHE[YEARLY_SHEET] = df
    return df


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
        "timeType": "年",
        "sheet": "Yearly_PressurePoint",
        "count": len(items),
        "data": items
    })
