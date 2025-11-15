# -*- coding: utf-8 -*-
"""
厂内-清水处理段（供水段）相关接口

- 清水处理段碳排信息   /api/process/inner/清水处理/info   (GET)
- 清水处理段碳排趋势   /api/process/inner/清水处理/trend  (POST)
- 清水处理段碳排占比   /api/process/inner/清水处理/share  (GET)

Excel：data/范围2_水厂内外_分段与单元.xlsx
sheet：
  - 水厂外_分段   => 按工艺段汇总（trend 用）
  - 水厂外_分单元 => 按工艺单元汇总（info/share 用）
"""

import os
from typing import Optional, Dict, Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()


# ========= 入参模型 =========

class TrendBody(BaseModel):
    # 1=清水池, 2=普通水调节池（目前只在文档中区分，trend 已改为读分段总量）
    qtype: int
    # 1=日，2=周、3=月、4=年
    timeType: int


# ========= 全局缓存 =========

_SECTION_TABLE: Optional[pd.DataFrame] = None   # 水厂外_分段
_UNIT_TABLE: Optional[pd.DataFrame] = None      # 水厂外_分单元

EXCEL_PATH = os.path.join("data", "范围2_水厂内外_分段与单元.xlsx")

# 这里的工艺段名称按你的说明使用“供水段”
SEG_NAME = "供水段"

# timeType -> “日/周/月/年”
TIME_MAP: Dict[int, str] = {
    1: "日",
    2: "周",
    3: "月",
    4: "年",
}

# timeType -> 合计列名
TIME_COL_MAP: Dict[int, str] = {
    1: "合计_日",
    2: "合计_周",
    3: "合计_月",
    4: "合计_年",
}


# ========= 读表工具函数 =========

def load_section_table() -> pd.DataFrame:
    """读取 sheet《水厂外_分段》（仅加载一次，后续使用缓存）"""
    global _SECTION_TABLE
    if _SECTION_TABLE is not None:
        return _SECTION_TABLE

    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name="水厂外_分段")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Excel(水厂外_分段)加载失败: {e}")

    required_cols = [
        "工艺段",
        "合计_日", "合计_周", "合计_月", "合计_年",
        "电耗_日", "电耗_周", "电耗_月", "电耗_年",
        "药耗_日", "药耗_周", "药耗_月", "药耗_年",
    ]
    for c in required_cols:
        if c not in df.columns:
            raise HTTPException(status_code=500, detail=f"水厂外_分段 缺少字段：{c}")

    # 数值列转 float
    for prefix in ["合计", "电耗", "药耗"]:
        for suff in ["日", "周", "月", "年"]:
            col = f"{prefix}_{suff}"
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    _SECTION_TABLE = df
    return df


def load_unit_table() -> pd.DataFrame:
    """读取 sheet《水厂外_分单元》（仅加载一次，后续使用缓存）"""
    global _UNIT_TABLE
    if _UNIT_TABLE is not None:
        return _UNIT_TABLE

    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name="水厂外_分单元")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Excel(水厂外_分单元)加载失败: {e}")

    if "工艺段" not in df.columns or "工艺单元" not in df.columns:
        raise HTTPException(status_code=500, detail="水厂外_分单元 缺少字段：工艺段/工艺单元")

    # 把合计/电耗/药耗/段内占比的日周月年列全部转成 float
    for prefix in ["合计", "电耗", "药耗", "段内占比"]:
        for suff in ["日", "周", "月", "年"]:
            col = f"{prefix}_{suff}"
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    _UNIT_TABLE = df
    return df


# ========= 公共小函数 =========

def _period_suffix(time_type: int) -> str:
    """timeType -> ‘日/周/月/年’ 后缀"""
    if time_type not in TIME_MAP:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")
    return TIME_MAP[time_type]


def _select_supply_units() -> pd.DataFrame:
    """从《水厂外_分单元》中筛选供水段的所有单元（info/share 用）"""
    df = load_unit_table()
    supply = df[df["工艺段"] == SEG_NAME].copy()
    if supply.empty:
        raise HTTPException(status_code=500, detail=f"未在水厂外_分单元中找到“{SEG_NAME}”记录")
    return supply


def _select_supply_section() -> pd.DataFrame:
    """从《水厂外_分段》中筛选供水段（trend 用）"""
    df = load_section_table()
    seg = df[df["工艺段"] == SEG_NAME].copy()
    if seg.empty:
        raise HTTPException(status_code=500, detail=f"未在水厂外_分段中找到“{SEG_NAME}”记录")
    return seg


# ========= 1) 厂内-清水处理段-碳排信息（供水段） =========

@router.get("/api/process/inner/清水处理/info")
def clearwater_info(
    timeType: int = Query(
        4, description="1=日, 2=周, 3=月, 4=年（默认按年）"
    )
) -> Dict[str, Any]:
    """
    清水处理（供水段）碳排信息（数据来源：水厂外_分单元）：

    - totalCarbonEmissions:
        工艺段 = 供水段 的所有单元，在合计_日/周/月/年中的合计
    - clearWaterTankCE:
        工艺单元包含 “清水池” 的合计
    - ordinaryWaterRegulationCE:
        工艺单元包含 “普通水调节” 的合计
    """
    if timeType not in TIME_COL_MAP:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    df = _select_supply_units()
    col = TIME_COL_MAP[timeType]

    # 段总碳排
    total = float(df[col].sum())

    # 清水池
    clear_tank = df[df["工艺单元"].astype(str).str.contains("清水池")]
    clear_tank_ce = float(clear_tank[col].sum())

    # 普通水调节池
    ordinary_reg = df[df["工艺单元"].astype(str).str.contains("普通水调节")]
    ordinary_reg_ce = float(ordinary_reg[col].sum())

    return {
        "code": 0,
        "msg": "",
        "data": {
            "totalCarbonEmissions": total,
            "clearWaterTankCE": clear_tank_ce,
            "ordinaryWaterRegulationCE": ordinary_reg_ce,
        },
    }


# ========= 2) 厂内-清水处理段-碳排趋势 =========

@router.post("/api/process/inner/清水处理/trend")
def clearwater_trend(body: TrendBody) -> Dict[str, Any]:
    """
    清水处理（供水段）整体碳排趋势

    qtype:
      1 = 清水池
      2 = 普通水调节池
      （目前仅占位，计算使用“供水段”整体分段数据）
    timeType:
      1=日，2=周、3=月、4=年

    数据来源：sheet《水厂外_分段》中“供水段”这一行
      - 总碳排: 合计_*
      - 电耗碳排: 电耗_*
      - 药耗碳排: 药耗_*
    """
    suffix = _period_suffix(body.timeType)  # 日/周/月/年
    col_total = f"合计_{suffix}"
    col_elec = f"电耗_{suffix}"
    col_chem = f"药耗_{suffix}"

    df = _select_supply_section()

    for c in [col_total, col_elec, col_chem]:
        if c not in df.columns:
            raise HTTPException(status_code=500, detail=f"水厂外_分段 缺少字段：{c}")

    total_val = float(df[col_total].sum())
    elec_val = float(df[col_elec].sum())
    chem_val = float(df[col_chem].sum())

    period_label = suffix  # “日/周/月/年” 作为 x 轴标签

    return {
        "code": 0,
        "msg": "",
        "data": {
            "id": "80",
            "styleType": "0",
            "customOption": {},
            "xAxis": [
                {
                    "type": "category",
                    "name": "",
                    "data": [period_label],
                }
            ],
            "yAxis": [
                {
                    "name": "kgCO₂e",
                    "type": "value",
                }
            ],
            "series": [
                {
                    "name": "总碳排",
                    "type": "line",
                    "data": [total_val],
                },
                {
                    "name": "电耗碳排",
                    "type": "line",
                    "data": [elec_val],
                },
                {
                    "name": "药耗碳排",
                    "type": "line",
                    "data": [chem_val],
                },
            ],
            "colors": [
                "#4992ff",
                "#7cffb2",
                "#dd79ff",
                "#fddd60",
                "#ff6e76",
                "#58d9f9",
                "#05c091",
                "#ff8a45",
                "#8d48e3",
            ],
        },
    }


# ========= 3) 厂内-清水处理段-碳排占比 =========

@router.get("/api/process/inner/清水处理/share")
def clearwater_share(timeType: int = 4) -> Dict[str, Any]:
    """
    清水处理（供水段）内部构成占比（使用《水厂外_分单元》中“段内占比_日/周/月/年”列）

    timeType:
      1 = 日 -> 段内占比_日
      2 = 周 -> 段内占比_周
      3 = 月 -> 段内占比_月
      4 = 年 -> 段内占比_年（默认）

    维度：
      - 清水池碳排
      - 普通水调节池碳排
      - 送水泵房碳排
    """
    suffix = _period_suffix(timeType)
    col_ratio = f"段内占比_{suffix}"

    df = _select_supply_units()  # 只看工艺段 = 供水段

    if col_ratio not in df.columns:
        raise HTTPException(status_code=500, detail=f"水厂外_分单元 缺少字段：{col_ratio}")

    clear_tank = df[df["工艺单元"].astype(str).str.contains("清水池")]
    ordinary_reg = df[df["工艺单元"].astype(str).str.contains("普通水调节")]
    pump_house = df[df["工艺单元"].astype(str).str.contains("送水泵房")]

    def _sum_ratio(d: pd.DataFrame) -> float:
        if d.empty:
            return 0.0
        return float(pd.to_numeric(d[col_ratio], errors="coerce").fillna(0.0).sum())

    clear_tank_ratio = _sum_ratio(clear_tank)
    ordinary_reg_ratio = _sum_ratio(ordinary_reg)
    pump_house_ratio = _sum_ratio(pump_house)

    source = [
        {"工艺单元": "清水池碳排", "数据值": clear_tank_ratio},
        {"工艺单元": "普通水调节池碳排", "数据值": ordinary_reg_ratio},
        {"工艺单元": "送水泵房碳排", "数据值": pump_house_ratio},
    ]

    return {
        "code": 0,
        "msg": "",
        "data": {
            "dimensions": ["工艺单元", "数据值"],
            "source": source,
            "dimensionsMapping": ["工艺单元", "数据值"],
        },
    }
