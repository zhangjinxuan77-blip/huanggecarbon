# -*- coding: utf-8 -*-
"""
碳排数据汇总接口
接口：POST /api/dashboard/scope-summary
入参：{"timeType":1}  # 1=日, 2=周, 3=月, 4=年
出参：按前端要求返回 xAxis / yAxis / data1~3
"""

from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .common import (
    load_table_from_excel,
    pick_scopes,
    standardize_period,
    TIME_MAP,
    format_float_2d,
)


router = APIRouter()


# ---------- 入参模型 ----------

class TimeBody(BaseModel):
    timeType: int  # 1=日, 2=周, 3=月, 4=年


# ---------- 表缓存与读取 ----------

_TABLE: Optional[pd.DataFrame] = None


def table() -> pd.DataFrame:
    """
    从 data/碳排总汇总.xlsx 读取“总汇总”sheet（或索引0），
    只保留：周期、范围1、范围2、范围3 四列，并做标准化。
    """
    global _TABLE
    if _TABLE is None:
        # 按需要修改 sheet_index：0 / 1 或者 "总汇总"
        df = load_table_from_excel("碳排总汇总.xlsx", sheet_index=0)

        # 自动识别 周期 / 范围1 / 范围2 / 范围3 列
        pc, s1c, s2c, s3c = pick_scopes(df)

        use = df[[pc, s1c, s2c, s3c]].copy()
        use.columns = ["周期", "范围1", "范围2", "范围3"]

        # 数值化
        for k in ["范围1", "范围2", "范围3"]:
            use[k] = pd.to_numeric(use[k], errors="coerce").fillna(0.0)

        # 周期统一为 "日/周/月/年"
        use["周期"] = use["周期"].map(standardize_period)

        _TABLE = use

    return _TABLE


# ---------- 业务逻辑 ----------

def build_payload(period: str) -> dict:
    """
    根据周期（"日/周/月/年"）构造前端需要的 payload。
    """
    tbl = table()
    row = tbl[tbl["周期"] == period]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Excel 未找到周期：{period}")

    v1, v2, v3 = [float(row.iloc[0][k]) for k in ["范围1", "范围2", "范围3"]]

    return {
        "code": 0,
        "msg": "",
        "data": {
            "xAxis": ["范围1", "范围2", "范围3"],
            "yAxis": ["直接排放", "间接排放"],
            "data1": [v1, 0.0],   # 范围1 → 直接排放
            "data2": [0.0, v2],   # 范围2 → 间接排放
            "data3": [0.0, v3],   # 范围3 → 间接排放
        },
    }


# ---------- 接口 ----------

@router.post("/api/dashboard/scope_summary")
def scope_summary(body: TimeBody):
    """
    入参：{"timeType":1}  # 1=日, 2=周, 3=月, 4=年
    """
    period = TIME_MAP.get(int(body.timeType))
    if not period:
        raise HTTPException(
            status_code=400,
            detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)",
        )
    return format_float_2d(build_payload(period))

