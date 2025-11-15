# -*- coding: utf-8 -*-
"""
厂内-过滤段相关接口
- 过滤段碳排信息       /api/process/inner/过滤/info          (GET)
- 过滤段碳排趋势       /api/process/inner/过滤/trend         (POST)
- 过滤段碳排占比       /api/process/inner/过滤/share         (GET)

Excel：data/范围2_水厂内外_分段与单元.xlsx
sheet：
  - 水厂内_分单元 => 按工艺单元汇总（info/share 用它）
  - 水厂内_分段   => 按工艺段汇总（trend 用它）
"""

import os
from typing import Optional, Dict, Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()


# ========= 入参模型 =========

class TrendBody(BaseModel):
    # 目前 qtype 对“过滤段趋势”接口已不再区分单元，仅占位，不参与计算
    qtype: int
    # 1=日，2=周、3=月、4=年
    timeType: int


# ========= 全局缓存 =========

_UNIT_TABLE: Optional[pd.DataFrame] = None   # 水厂内_分单元
_SECTION_TABLE: Optional[pd.DataFrame] = None  # 水厂内_分段

EXCEL_PATH = os.path.join("data", "范围2_水厂内外_分段与单元.xlsx")

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

def load_unit_table() -> pd.DataFrame:
    """读取 sheet《水厂内_分单元》（仅加载一次，后续使用缓存）"""
    global _UNIT_TABLE
    if _UNIT_TABLE is not None:
        return _UNIT_TABLE

    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name="水厂内_分单元")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Excel(水厂内_分单元)加载失败: {e}")

    if "工艺段" not in df.columns or "工艺单元" not in df.columns:
        raise HTTPException(status_code=500, detail="水厂内_分单元 缺少字段：工艺段/工艺单元")

    # 把合计/电耗/药耗/段内占比的日周月年列全部转成 float
    for prefix in ["合计", "电耗", "药耗", "段内占比"]:
        for suff in ["日", "周", "月", "年"]:
            col = f"{prefix}_{suff}"
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    _UNIT_TABLE = df
    return df


def load_section_table() -> pd.DataFrame:
    """读取 sheet《水厂内_分段》（仅加载一次，后续使用缓存）"""
    global _SECTION_TABLE
    if _SECTION_TABLE is not None:
        return _SECTION_TABLE

    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name="水厂内_分段")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Excel(水厂内_分段)加载失败: {e}")

    if "工艺段" not in df.columns:
        raise HTTPException(status_code=500, detail="水厂内_分段 缺少字段：工艺段")

    # 把合计/电耗/药耗的日周月年列全部转成 float
    for prefix in ["合计", "电耗", "药耗"]:
        for suff in ["日", "周", "月", "年"]:
            col = f"{prefix}_{suff}"
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    _SECTION_TABLE = df
    return df


# ========= 公共小函数 =========

def _period_suffix(time_type: int) -> str:
    """timeType -> ‘日/周/月/年’ 后缀"""
    if time_type not in TIME_MAP:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")
    return TIME_MAP[time_type]


def _select_filter_units() -> pd.DataFrame:
    """从《水厂内_分单元》中筛选过滤段的所有单元（info/share 用）"""
    df = load_unit_table()
    flt = df[df["工艺段"] == "过滤段"].copy()
    if flt.empty:
        raise HTTPException(status_code=500, detail="未在水厂内_分单元中找到“过滤段”记录")
    return flt


def _select_filter_section() -> pd.DataFrame:
    """从《水厂内_分段》中筛选过滤段（trend 用）"""
    df = load_section_table()
    flt = df[df["工艺段"] == "过滤段"].copy()
    if flt.empty:
        raise HTTPException(status_code=500, detail="未在水厂内_分段中找到“过滤段”记录")
    return flt


# ========= 1) 厂内-过滤段-碳排信息 =========

@router.get("/api/process/inner/过滤/info")
def filter_info(
    timeType: int = Query(
        4, description="1=日, 2=周, 3=月, 4=年（默认按年）"
    )
) -> Dict[str, Any]:
    """
    过滤段碳排信息（数据来源：水厂内_分单元）：
    - totalCarbonEmissions:
        工艺段 = 过滤段 的所有单元，在合计_日/周/月/年中的合计
    - flipPlateSandFilterCE:
        工艺单元 = 翻板砂滤池 的合计
    - sandFilterBackwashPumpHouseCE:
        工艺单元 = 砂滤反冲洗泵房 的合计
    - carbonFilterBackwashPumpHouseCE:
        工艺单元 = 炭滤反冲洗泵房 的合计
    """
    if timeType not in TIME_COL_MAP:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    df = _select_filter_units()
    col = TIME_COL_MAP[timeType]

    # 段总碳排
    total = float(df[col].sum())

    # 翻板砂滤池
    flip_row = df[df["工艺单元"] == "翻板砂滤池"]
    flip_ce = float(flip_row[col].sum())

    # 砂滤反冲洗泵房
    sand_row = df[df["工艺单元"] == "砂滤反冲洗泵房"]
    sand_ce = float(sand_row[col].sum())

    # 炭滤反冲洗泵房
    carbon_row = df[df["工艺单元"] == "炭滤反冲洗泵房"]
    carbon_ce = float(carbon_row[col].sum())

    return {
        "code": 0,
        "msg": "",
        "data": {
            "totalCarbonEmissions": total,
            "flipPlateSandFilterCE": flip_ce,
            "sandFilterBackwashPumpHouseCE": sand_ce,
            "carbonFilterBackwashPumpHouseCE": carbon_ce,
        },
    }


# ========= 2) 厂内-过滤段-碳排趋势 =========

@router.post("/api/process/inner/过滤/trend")
def filter_trend(body: TrendBody) -> Dict[str, Any]:
    """
    过滤段整体碳排趋势（不再按工艺单元拆开）

    qtype:
      目前忽略，仅占位（1/2/3 都可以传）
    timeType:
      1=日，2=周、3=月、4=年

    数据来源：sheet《水厂内_分段》中“过滤段”这一行
      - 总碳排: 合计_*
      - 电耗碳排: 电耗_*
      - 药耗碳排: 药耗_*
    """
    suffix = _period_suffix(body.timeType)  # 日/周/月/年
    col_total = f"合计_{suffix}"
    col_elec = f"电耗_{suffix}"
    col_chem = f"药耗_{suffix}"

    df = _select_filter_section()

    for c in [col_total, col_elec, col_chem]:
        if c not in df.columns:
            raise HTTPException(status_code=500, detail=f"水厂内_分段 缺少字段：{c}")

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


# ========= 3) 厂内-过滤段-碳排占比 =========

@router.get("/api/process/inner/过滤/share")
def filter_share(timeType: int = 4) -> Dict[str, Any]:
    """
    过滤段内部构成占比（使用《水厂内_分单元》中“段内占比_日/周/月/年”列）

    timeType:
      1 = 日 -> 段内占比_日
      2 = 周 -> 段内占比_周
      3 = 月 -> 段内占比_月
      4 = 年 -> 段内占比_年（默认）

    维度：
      - 翻板砂滤池碳排
      - 砂滤反冲洗泵房碳排
      - 炭滤反冲洗泵房碳排
    """
    suffix = _period_suffix(timeType)
    col_ratio = f"段内占比_{suffix}"

    df = _select_filter_units()

    if col_ratio not in df.columns:
        raise HTTPException(status_code=500, detail=f"水厂内_分单元 缺少字段：{col_ratio}")

    flip_row = df[df["工艺单元"] == "翻板砂滤池"]
    sand_row = df[df["工艺单元"] == "砂滤反冲洗泵房"]
    carbon_row = df[df["工艺单元"] == "炭滤反冲洗泵房"]

    def _sum_ratio(d: pd.DataFrame) -> float:
        if d.empty:
            return 0.0
        return float(pd.to_numeric(d[col_ratio], errors="coerce").fillna(0.0).sum())

    flip_ratio = _sum_ratio(flip_row)
    sand_ratio = _sum_ratio(sand_row)
    carbon_ratio = _sum_ratio(carbon_row)

    source = [
        {"工艺单元": "翻板砂滤池碳排", "数据值": flip_ratio},
        {"工艺单元": "砂滤反冲洗泵房碳排", "数据值": sand_ratio},
        {"工艺单元": "炭滤反冲洗泵房碳排", "数据值": carbon_ratio},
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
