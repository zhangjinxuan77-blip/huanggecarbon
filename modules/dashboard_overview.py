# -*- coding: utf-8 -*-
"""
数字概览接口
接口：POST /api/dashboard/overview
入参：{"timeType":1}   # 1=日, 2=周（你也可以扩展到 3=月, 4=年）
出参：
{
  "code": 0,
  "msg": "",
  "data": {
    "monthTotalTp": ...,       # 总碳排，吨
    "proWaterVolume": ...,     # 处理水量，m3
    "proUnitWater": ...,       # 单位水处理强度，kgCO2e/m3
    "emissionReduction": ...   # 预计减排量，吨
  }
}
"""

import os
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

# 1=日, 2=周, 3=月, 4=年（你文档只写了 1/2，这里多给两个不影响）
TIME_MAP = {1: "日", 2: "周", 3: "月", 4: "年"}


class TimeBody(BaseModel):
    timeType: int


_TABLE: Optional[pd.DataFrame] = None


def load_overview_table() -> pd.DataFrame:
    """只读一次 Excel，后面走缓存。"""
    global _TABLE
    if _TABLE is not None:
        return _TABLE

    # 项目结构：carbon_api_multi/
    #   ├─ app.py
    #   ├─ data/
    #   │    └─ 碳排_总汇总_含强度与减排.xlsx
    #   └─ modules/
    base_dir = os.path.dirname(os.path.dirname(__file__))
    data_dir = os.path.join(base_dir, "data")

    # 确认文件名跟你本地一致（建议去掉中间那个空格）
    excel_path = os.path.join(data_dir, "碳排_总汇总_含强度与减排.xlsx")
    if not os.path.exists(excel_path):
        # 兜底再试一下有空格的版本
        alt = os.path.join(data_dir, "碳排_总汇总_含强度与减排 .xlsx")
        if os.path.exists(alt):
            excel_path = alt
        else:
            raise HTTPException(500, f"未找到 Excel 文件：{excel_path}")

    # 只有一个 sheet：总汇总_含强度
    df = pd.read_excel(excel_path, sheet_name=0)

    # 清洗一下列名（防止有空格、全角括号等）
    df.columns = [
        str(c).strip().replace("（", "(").replace("）", ")")
        for c in df.columns
    ]

    _TABLE = df
    return _TABLE


def build_overview(period: str) -> dict:
    """从 DataFrame 中抽取对应“周期”的一行，拼成前端需要的结构。"""
    df = load_overview_table()

    # 周期一列：日/周/月/年
    ser = df["周期"].astype(str).str.strip()
    row = df[ser == period]
    if row.empty:
        raise HTTPException(404, f"Excel 未找到周期：{period}")

    r = row.iloc[0]

    try:
        total_kg = float(r["总碳排_kgCO2e"])
        water_m3 = float(r["水处理量_m3"])
        unit_intensity = float(r["单位水处理强度_kgCO2e_per_m3"])
        reduction_kg = float(r["预计减排量_kgCO2e"])
    except KeyError as e:
        # 某一列名字对不上时的报错提示
        raise HTTPException(500, f"Excel 数据缺失或列名错误：{e}")

    # 把 kg 转成 吨（和前端示例的 monthTotalTp 吨 对齐）
    month_total_tp_ton = total_kg / 1000.0
    reduction_ton = reduction_kg / 1000.0

    return {
        "code": 0,
        "msg": "",
        "data": {
            "monthTotalTp": month_total_tp_ton,
            "proWaterVolume": water_m3,
            "proUnitWater": unit_intensity,
            "emissionReduction": reduction_ton,
        },
    }


@router.post("/api/dashboard/overview")
def overview(body: TimeBody):
    if body.timeType not in TIME_MAP:
        raise HTTPException(400, "timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    period = TIME_MAP[body.timeType]
    return build_overview(period)
