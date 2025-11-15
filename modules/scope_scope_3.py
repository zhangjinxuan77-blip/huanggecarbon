# -*- coding: utf-8 -*-
"""
范围3-总碳排 /api/scope/scope_3

Excel: data/Scope3_含分项.xlsx
Sheet: "Scope3_汇总(日周月年)"
"""

import os
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()


class TimeBody(BaseModel):
    timeType: int  # 1=日，2=周、3=月、4=年


# 映射：1/2/3/4 → 周期中文
TIME_MAP = {
    1: "日",
    2: "周",
    3: "月",
    4: "年",
}

_SCOPE3_TABLE: Optional[pd.DataFrame] = None


def load_scope3_table() -> pd.DataFrame:
    """读取 Scope3_含分项.xlsx 的 Scope3_汇总(日周月年)，并缓存"""
    global _SCOPE3_TABLE

    if _SCOPE3_TABLE is not None:
        return _SCOPE3_TABLE

    excel_path = os.path.join("data", "Scope3_含分项.xlsx")
    try:
        df = pd.read_excel(excel_path, sheet_name="Scope3_汇总(日周月年)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Excel 加载失败: {e}")

    # === 按你表里的真实列名来 ===
    required_cols = [
        "周期",
        "范围3总碳排放量(kgCO2e)",
        "药剂碳排总量(kgCO2e)",
        "污泥运输碳排放量(kgCO2e)",   # 这里改成“碳排放量”
        "药剂碳排占比",
        "污泥运输占比",
    ]
    for c in required_cols:
        if c not in df.columns:
            raise HTTPException(status_code=500, detail=f"Excel 缺少字段：{c}")

    # 数值列统一转成 float
    for c in required_cols[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    _SCOPE3_TABLE = df
    return df


def build_payload(period: str) -> dict:
    df = load_scope3_table()

    row = df[df["周期"] == period]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Excel 未找到周期：{period}")

    r = row.iloc[0]

    total = float(r["范围3总碳排放量(kgCO2e)"])
    chem = float(r["药剂碳排总量(kgCO2e)"])
    sludge = float(r["污泥运输碳排放量(kgCO2e)"])  # 这里也用“碳排放量”

    share_chem = float(r["药剂碳排占比"])
    share_sludge = float(r["污泥运输占比"])

    return {
        "code": 0,
        "msg": "",
        "data": {
            # 总碳排量 + 各分项 + 占比
            "totalCarbonEmissions": total,
            "carbonEmissionsChemicalAgents": chem,
            "carbonEmissionsSludgeTransportation": sludge,
            "shareChemicalAgents": share_chem,
            "shareSludgeTransportation": share_sludge,
            # 给前端的结构饼图
            "chart": {
                "dimensions": ["name", "data"],
                "source": [
                    {"name": "药剂碳排", "data": chem},
                    {"name": "污泥运输", "data": sludge},
                ],
            },
        },
    }


@router.post("/api/scope/scope_3")
def scope_3_total(body: TimeBody):
    period = TIME_MAP.get(body.timeType)
    if not period:
        raise HTTPException(
            status_code=400,
            detail="timeType 只能为 1(日)/2(周)/3(月)/4(年)",
        )
    return build_payload(period)
