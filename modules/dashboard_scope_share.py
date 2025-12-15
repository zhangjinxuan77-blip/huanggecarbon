# -*- coding: utf-8 -*-
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd
from modules.common import format_float_2d
router = APIRouter()

# 入参
class TimeBody(BaseModel):
    timeType: int  # 1=日、2=周、3=月、4=年

# 频率映射
PERIOD_MAP = {
    1: "日",
    2: "周",
    3: "月",
    4: "年"
}

# 缓存
_TABLE = None


def load_table():
    global _TABLE
    if _TABLE is not None:
        return _TABLE

    try:
        df = pd.read_excel("data/碳排总汇总.xlsx", sheet_name=0)
    except Exception as e:
        raise HTTPException(500, f"Excel 加载失败: {e}")

    needed_cols = ["周期", "范围1占比(%)", "范围2占比(%)", "范围3占比(%)"]
    for c in needed_cols:
        if c not in df.columns:
            raise HTTPException(500, f"Excel 缺少字段：{c}")

    _TABLE = df
    return df


@router.post("/api/dashboard/scope_share")
def scope_share(body: TimeBody):

    df = load_table()

    # 根据 timeType找到周期（日/周/月/年）
    period = PERIOD_MAP.get(body.timeType)
    if not period:
        raise HTTPException(400, "timeType 必须是 1(日)/2(周)/3(月)/4(年)")

    row = df[df["周期"] == period]
    if row.empty:
        raise HTTPException(404, f"Excel 未找到周期：{period}")

    row = row.iloc[0]

    p1 = float(row["范围1占比(%)"])
    p2 = float(row["范围2占比(%)"])
    p3 = float(row["范围3占比(%)"])

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "dimensions": ["name", "data"],
            "source": [
                {"name": "范围1", "data": p1},
                {"name": "范围2", "data": p2},
                {"name": "范围3", "data": p3},
            ],
            "dimensionsMapping": ["name", "data"]
        }
    })
