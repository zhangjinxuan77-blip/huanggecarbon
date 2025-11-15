# -*- coding: utf-8 -*-
"""
厂内-混凝沉淀段相关接口
- 混凝沉淀段碳排信息       /api/process/inner/混凝沉淀/info          (GET)
- 混凝沉淀段碳排趋势       /api/process/inner/混凝沉淀/trend         (POST)
- 混凝沉淀段碳排占比       /api/process/inner/混凝沉淀/share         (GET)

Excel：data/范围2_水厂内外_分段与单元.xlsx
sheet：
  - 水厂内_分单元 => 按工艺单元汇总
"""

import os
from typing import Optional, Dict, Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()

# ========= 入参模型 =========

class TrendBody(BaseModel):
    qtype: int      # 1=折板反应平流沉淀池（目前只这一类）
    timeType: int   # 1=日，2=周、3=月、4=年


# ========= 全局缓存 =========

_UNIT_TABLE: Optional[pd.DataFrame] = None      # 水厂内_分单元

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


# ========= 公共小函数 =========

def _period_suffix(time_type: int) -> str:
    """timeType -> ‘日/周/月/年’ 后缀"""
    if time_type not in TIME_MAP:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")
    return TIME_MAP[time_type]


def _select_mixing_units() -> pd.DataFrame:
    """
    从《水厂内_分单元》中筛选混凝沉淀段的所有单元
    """
    df = load_unit_table()
    mixing = df[df["工艺段"] == "混凝沉淀段"].copy()
    if mixing.empty:
        raise HTTPException(status_code=500, detail="未在水厂内_分单元中找到“混凝沉淀段”记录")
    return mixing


# ========= 1) 厂内-混凝沉淀段-碳排信息 =========

@router.get("/api/process/inner/混凝沉淀/info")
def mixing_info(
    timeType: int = Query(
        4, description="1=日, 2=周, 3=月, 4=年（默认按年）"
    )
) -> Dict[str, Any]:
    """
    混凝沉淀段碳排信息（数据来源：水厂内_分单元）：
    - totalCarbonEmissions:
        工艺段 = 混凝沉淀段 的所有单元，在合计_日/周/月/年中的合计
    - foldablePlateReactionHorizontalFlowSedimentationTankCE:
        工艺段 = 混凝沉淀段，工艺单元 = 折板反应平流沉淀池 的合计
    """
    if timeType not in TIME_COL_MAP:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    df = _select_mixing_units()
    col = TIME_COL_MAP[timeType]

    # 混凝沉淀段总碳排（所有单元求和）
    total = float(df[col].sum())

    # 折板反应平流沉淀池 —— 直接取这一行的值
    fold_row = df[df["工艺单元"] == "折板反应平流沉淀池"]
    if not fold_row.empty:
        fold_ce = float(fold_row[col].iloc[0])
    else:
        fold_ce = 0.0

    return {
        "code": 0,
        "msg": "",
        "data": {
            "totalCarbonEmissions": total,
            "foldablePlateReactionHorizontalFlowSedimentationTankCE": fold_ce,
        },
    }


# ========= 2) 厂内-混凝沉淀段-碳排趋势 =========

@router.post("/api/process/inner/混凝沉淀/trend")
def mixing_trend(body: TrendBody) -> Dict[str, Any]:
    """
    qtype:
      1 = 折板反应平流沉淀池
    timeType:
      1=日，2=周、3=月、4=年

    目前 Excel 是“日/周/月/年”的汇总值，这里按选定周期返回一组数据点：
      xAxis: [ "日" / "周" / "月" / "年" ]
      series:
        - 总碳排
        - 电耗碳排
        - 药耗碳排
    """
    if body.qtype != 1:
        raise HTTPException(status_code=400, detail="qtype 目前只支持 1(折板反应平流沉淀池)")

    suffix = _period_suffix(body.timeType)  # 日/周/月/年
    df = _select_mixing_units()

    target_df = df[df["工艺单元"] == "折板反应平流沉淀池"]
    if target_df.empty:
        raise HTTPException(
            status_code=404,
            detail="未在水厂内_分单元中找到“折板反应平流沉淀池”的数据",
        )

    col_total = f"合计_{suffix}"
    col_elec = f"电耗_{suffix}"
    col_chem = f"药耗_{suffix}"

    for c in [col_total, col_elec, col_chem]:
        if c not in target_df.columns:
            raise HTTPException(status_code=500, detail=f"水厂内_分单元 缺少字段：{c}")

    total_val = float(target_df[col_total].sum())
    elec_val = float(target_df[col_elec].sum())
    chem_val = float(target_df[col_chem].sum())

    period_label = suffix  # 直接用“日/周/月/年”作为 x 轴标签

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


# ========= 3) 厂内-混凝沉淀段-碳排占比 =========

@router.get("/api/process/inner/混凝沉淀/share")
def mixing_share(timeType: int = 4) -> Dict[str, Any]:
    """
    混凝沉淀段内部构成占比（使用《水厂内_分单元》中“段内占比_日/周/月/年”列）

    timeType:
      1 = 日 -> 段内占比_日
      2 = 周 -> 段内占比_周
      3 = 月 -> 段内占比_月
      4 = 年 -> 段内占比_年（默认）

    特别约定：
      - “折板反应平流沉淀池” 单独一类
      - 所有药耗工艺单元里，只要名称包含 “PAC” 的，都合并为 “PAC投加”
    """
    suffix = _period_suffix(timeType)          # 日 / 周 / 月 / 年
    col_ratio = f"段内占比_{suffix}"

    df = _select_mixing_units()

    if col_ratio not in df.columns:
        raise HTTPException(status_code=500, detail=f"水厂内_分单元 缺少字段：{col_ratio}")

    # 折板反应平流沉淀池
    fold = df[df["工艺单元"] == "折板反应平流沉淀池"]

    # PAC 投加相关单元：名称中包含“PAC”
    pac = df[df["工艺单元"].astype(str).str.contains("PAC")]

    def _sum_ratio(d: pd.DataFrame) -> float:
        if d.empty:
            return 0.0
        return float(pd.to_numeric(d[col_ratio], errors="coerce").fillna(0.0).sum())

    fold_ratio = _sum_ratio(fold)
    pac_ratio = _sum_ratio(pac)

    source = [
        {"碳排结构": "折板反应平流沉淀池", "数据值": fold_ratio},
        {"碳排结构": "PAC投加", "数据值": pac_ratio},
    ]

    return {
        "code": 0,
        "msg": "",
        "data": {
            "dimensions": ["碳排结构", "数据值"],
            "source": source,
            "dimensionsMapping": ["碳排结构", "数据值"],
        },
    }
