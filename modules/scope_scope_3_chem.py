# -*- coding: utf-8 -*-
"""
范围3-药剂碳排 /api/scope/scope_3/chem

Excel：data/Scope3_含分项.xlsx
sheet：Scope3_分项(日周月年)
"""

import os
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()


class TimeBody(BaseModel):
    timeType: int  # 1=日，2=周、3=月、4=年


TIME_MAP = {
    1: "日",
    2: "周",
    3: "月",
    4: "年",
}

_CHEM_TABLE: Optional[pd.DataFrame] = None


def load_chem_table() -> pd.DataFrame:
    global _CHEM_TABLE
    if _CHEM_TABLE is not None:
        return _CHEM_TABLE

    excel_path = os.path.join("data", "Scope3_含分项.xlsx")
    try:
        df = pd.read_excel(excel_path, sheet_name="Scope3_分项(日周月年)")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Excel 加载失败: %s" % e)

    required_cols = ["周期", "药剂或污泥运输", "碳排放量(kgCO2e)"]
    for col in required_cols:
        if col not in df.columns:
            raise HTTPException(status_code=500, detail="Excel 缺少字段：%s" % col)

    df["碳排放量(kgCO2e)"] = pd.to_numeric(df["碳排放量(kgCO2e)"], errors="coerce").fillna(0.0)

    dose_col = None
    for c in df.columns:
        if "投加量" in str(c):
            dose_col = c
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
            break
    df["_dose_col_"] = dose_col

    _CHEM_TABLE = df
    return df


def build_payload(period: str) -> dict:
    df = load_chem_table()
    sub = df[df["周期"] == period].copy()
    if sub.empty:
        raise HTTPException(status_code=404, detail="Excel 未找到周期：%s" % period)

    chems_order = ["O3", "次氯酸钠", "PAC", "PAM"]

    dose_col = sub["_dose_col_"].iloc[0]
    has_dose = isinstance(dose_col, str) and dose_col in sub.columns

    doses = []
    emissions = []

    for name in chems_order:
        row = sub[sub["药剂或污泥运输"] == name]
        if row.empty:
            carbon = 0.0
            dose = 0.0
        else:
            carbon = float(row["碳排放量(kgCO2e)"].iloc[0])
            dose = float(row[dose_col].iloc[0]) if has_dose else 0.0

        emissions.append(carbon)
        doses.append(dose)

    total = float(sum(emissions))

    return {
        "code": 0,
        "msg": "",
        "data": {
            "totalCarbonEmissionsChemicalAgents": total,
            "chart": {
                "xAxis": ["投加量", "碳排放量"],
                "yAxis": chems_order,
                "data1": doses,
                "data2": emissions,
            },
        },
    }


@router.post("/api/scope/scope_3/chem")
def scope_3_chem(body: TimeBody):
    period = TIME_MAP.get(int(body.timeType))
    if not period:
        raise HTTPException(
            status_code=400,
            detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)",
        )
    return build_payload(period)
