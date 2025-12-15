# -*- coding: utf-8 -*-
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd
from typing import Optional
from modules.common import format_float_2d
router = APIRouter()

# ====== 入参 ======
class TimeBody(BaseModel):
    timeType: int  # 1=日、2=周、3=月、4=年


# ====== 缓存（避免重复读 Excel） ======
_TABLE: Optional[pd.DataFrame] = None


# ====== 表加载函数 ======
def load_table() -> pd.DataFrame:
    """
    读取《碳排_总汇总_含强度与减排.xlsx》
    需要其中的 sheet：'总汇总_含强度'
    """
    global _TABLE
    if _TABLE is not None:
        return _TABLE

    try:
        df = pd.read_excel("data/碳排_总汇总_含强度与减排.xlsx",
                           sheet_name="总汇总_含强度")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Excel 加载失败: {e}")

    # 要求 DF 至少包含如下列：
    required_cols = ["周期", "水处理量_m3", "单位水处理强度_kgCO2e_per_m3"]
    for c in required_cols:
        if c not in df.columns:
            raise HTTPException(status_code=500, detail=f"Excel 缺少字段：{c}")

    _TABLE = df
    return df


# ====== 主接口 ======
@router.post("/api/dashboard/unit_intensity")
def unit_intensity(body: TimeBody):
    """
    入参：
        {"timeType": 1}  # 1=日, 2=周, 3=月, 4=年（目前只是占位，不区分）
    出参：
        {
          "code": 0,
          "msg": "",
          "data": {
            "dimensions": ["总处理水量", "单位处理强度"],
            "source": [...],
            "dimensionsMapping": ["总处理水量", "单位处理强度"]
          }
        }
    """
    if body.timeType not in (1, 2, 3, 4):
        raise HTTPException(status_code=400,
                            detail="timeType 必须是 1(日)/2(周)/3(月)/4(年)")

    df = load_table()

    # 每行输出一组数据（不再包含“时间”）
    source = []
    for _, row in df.iterrows():
        source.append({
            "总处理水量": float(row["水处理量_m3"]),
            "单位处理强度": float(row["单位水处理强度_kgCO2e_per_m3"]),
        })

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "dimensions": ["总处理水量", "单位处理强度"],
            "source": source,
            "dimensionsMapping": ["总处理水量", "单位处理强度"],
        }
    })
