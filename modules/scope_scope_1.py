# -*- coding: utf-8 -*-
"""
范围1 · O3泄漏接口
POST /api/scope/scope_1
入参: {"timeType":1}   # 1=日, 2=周, 3=月, 4=年
Excel: data/范围1_O3与范围3_污泥运输.xlsx
Sheet: Scope1_O3泄漏
列: period | O3泄漏碳排放量_kgCO2e | O3投加量_kg | O3泄漏量_kg
"""

import os
from typing import Optional
from modules.common import format_float_2d
import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

# timeType 映射
TIME_MAP = {
    1: "日",
    2: "周",
    3: "月",
    4: "年",
}


class TimeBody(BaseModel):
    timeType: int  # 1=日、2=周、3=月、4=年


# 简单缓存
_TABLE_CACHE: Optional[pd.DataFrame] = None


def load_table() -> pd.DataFrame:
    """读取 Scope1_O3泄漏 sheet 并做校验"""
    global _TABLE_CACHE
    if _TABLE_CACHE is not None:
        return _TABLE_CACHE

    # 项目根目录：.../carbon_api_multi
    base_dir = os.path.dirname(os.path.dirname(__file__))
    excel_path = os.path.join(base_dir, "data", "范围1_O3与范围3_污泥运输.xlsx")

    try:
        df = pd.read_excel(excel_path, sheet_name="Scope1_O3泄漏")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Excel 加载失败: {e}")

    # 注意这里的“漏”字
    required_cols = [
        "period",
        "O3泄漏碳排放量_kgCO2e",
        "O3投加量_kg",
        "O3泄漏量_kg",
    ]
    for c in required_cols:
        if c not in df.columns:
            raise HTTPException(status_code=500, detail=f"Excel 缺少字段：{c}")

    _TABLE_CACHE = df
    return df


@router.post("/api/scope/scope_1")
def scope_1(body: TimeBody):
    """
    返回:
    {
      "code": 0,
      "msg": "",
      "data": {
        "leakageCarbonEmissions": "...",
        "dosage": "...",
        "leakageAmount": "..."
      }
    }
    """
    period = TIME_MAP.get(int(body.timeType))
    if not period:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    df = load_table()
    row = df[df["period"] == period]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Excel 未找到周期：{period}")

    r = row.iloc[0]

    leakage_ce = float(r["O3泄漏碳排放量_kgCO2e"])
    dosage = float(r["O3投加量_kg"])
    leakage_amt = float(r["O3泄漏量_kg"])

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "leakageCarbonEmissions": f"{leakage_ce}",
            "dosage": f"{dosage}",
            "leakageAmount": f"{leakage_amt}",
        },
    })
