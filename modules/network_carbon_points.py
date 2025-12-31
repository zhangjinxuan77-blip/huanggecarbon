# -*- coding: utf-8 -*-
"""
管网碳排监测点接口（仅 日/周/月/年）
接口：POST /api/network/points-carbon
入参：{"timeType": 1}  # 1=日 2=周 3=月 4=年
出参：监测点名称（前端为准）、碳排量、监测时间（最新一条）
"""

import os
import pandas as pd
from typing import List, Dict, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

# base_dir=当前文件上上级目录
base_dir = os.path.dirname(os.path.dirname(__file__))
XLSX_PATH = os.path.join(base_dir, "data", "管网碳排_按压力监测点_坐标匹配.xlsx")

# timeType -> sheet 映射（1=日、2=周、3=月、4=年）
SHEET_MAP = {
    1: "Daily_PressurePoint",
    2: "Weekly_PressurePoint",
    3: "Monthly_PressurePoint",
    4: "Yearly_PressurePoint",
}


class TimeBody(BaseModel):
    timeType: int  # 1=日 2=周 3=月 4=年


# 简单缓存（避免每次都读 Excel）
_CACHE: dict[int, pd.DataFrame] = {}


def _load_sheet(time_type: int) -> pd.DataFrame:
    if time_type not in SHEET_MAP:
        raise HTTPException(status_code=400, detail="timeType 必须为 1~4（日/周/月/年）")

    if not os.path.exists(XLSX_PATH):
        raise HTTPException(status_code=404, detail=f"未找到文件: {XLSX_PATH}")

    if time_type in _CACHE:
        return _CACHE[time_type]

    sheet = SHEET_MAP[time_type]
    try:
        df = pd.read_excel(XLSX_PATH, sheet_name=sheet)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取Excel失败: {repr(e)}")

    _CACHE[time_type] = df
    return df


def _latest_per_point(df: pd.DataFrame) -> pd.DataFrame:
    """
    每个 point 取最新一条记录
    - 日/周/月/年 sheet 通常用 period 列
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
def network_points_carbon(body: TimeBody) -> Dict[str, Any]:
    """
    返回前端地图点弹窗所需数据：
    - name: 监测点名称（前端为准）
    - carbonKg: 碳排量（kgCO2e）
    - monitorTime: 监测时间（字符串）
    """
    time_type = body.timeType
    df = _load_sheet(time_type)
    latest = _latest_per_point(df)

    items: List[Dict[str, Any]] = []
    time_col = "period" if "period" in latest.columns else "ts"

    for _, r in latest.iterrows():
        name = str(r["point"])
        carbon = r["CO2e_kg"]
        t = r[time_col]

        items.append({
            "name": name,
            "carbonKg": 0.0 if pd.isna(carbon) else float(carbon),
            "monitorTime": "" if pd.isna(t) else t.strftime("%Y-%m-%d %H:%M:%S"),
        })

    return {
        "timeType": time_type,
        "sheet": SHEET_MAP[time_type],
        "count": len(items),
        "data": items
    }
