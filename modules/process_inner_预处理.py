# -*- coding: utf-8 -*-
"""
厂内-预处理段相关接口
- 预处理段碳排信息       /api/process/inner/预处理/info          (GET)
- 预处理段碳排趋势       /api/process/inner/预处理/trend         (POST)
- 预处理段碳排占比       /api/process/inner/预处理/share         (GET)

Excel：data/范围2_水厂内外_分段与单元.xlsx
sheet：
  - 水厂内_分段   => 按工艺段汇总
  - 水厂内_分单元 => 按工艺单元汇总
"""

import os
from typing import Optional, Dict, Any
from modules.common import format_float_2d

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()


# ========= 入参模型 =========

class TrendBody(BaseModel):
    qtype: int      # 1=配水井+预臭氧接触池；2=加药间
    timeType: int   # 1=日，2=周、3=月、4=年


# ========= 全局缓存 =========

_SECTION_TABLE: Optional[pd.DataFrame] = None   # 水厂内_分段
_UNIT_TABLE: Optional[pd.DataFrame] = None      # 水厂内_分单元

EXCEL_PATH = os.path.join("data", "范围2_水厂内外_分段与单元.xlsx")

# timeType -> “日/周/月/年”
TIME_MAP: Dict[int, str] = {
    1: "日",
    2: "周",
    3: "月",
    4: "年",
}

# timeType -> 水厂内_分单元里的合计列名
TIME_COL_MAP: Dict[int, str] = {
    1: "合计_日",
    2: "合计_周",
    3: "合计_月",
    4: "合计_年",
}


# ========= 读表工具函数 =========

def load_section_table() -> pd.DataFrame:
    """读取 sheet《水厂内_分段》（仅加载一次，后续使用缓存）"""
    global _SECTION_TABLE
    if _SECTION_TABLE is not None:
        return _SECTION_TABLE

    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name="水厂内_分段")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Excel(水厂内_分段)加载失败: {e}")

    required_cols = [
        "工艺段",
        "合计_日", "合计_周", "合计_月", "合计_年",
        "电耗_日", "电耗_周", "电耗_月", "电耗_年",
        "药耗_日", "药耗_周", "药耗_月", "药耗_年",
    ]
    for c in required_cols:
        if c not in df.columns:
            raise HTTPException(status_code=500, detail=f"水厂内_分段 缺少字段：{c}")

    _SECTION_TABLE = df
    return df


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

    # 把合计/电耗/药耗的日周月年列全部转成 float，避免后面求和出问题
    for prefix in ["合计", "电耗", "药耗"]:
        for suff in ["日", "周", "月", "年"]:
            col = f"{prefix}_{suff}"
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    _UNIT_TABLE = df
    return df


# ========= 公共小函数 =========

def _select_pretreat_units() -> Dict[str, pd.DataFrame]:
    """
    从《水厂内_分单元》中筛选预处理段下的两个单元：
    - 配水井和预臭氧接触池
    - 加药间
    返回 dict: {"well_ozone": df1, "hypo": df2}
    """
    df = load_unit_table()

    pretreat = df[df["工艺段"] == "预处理段"].copy()
    if pretreat.empty:
        raise HTTPException(status_code=500, detail="未在水厂内_分单元中找到“预处理段”记录")

    well_ozone = pretreat[
        pretreat["工艺单元"].astype(str).str.contains("配水井和预臭氧接触池")
    ]
    hypo = pretreat[
        pretreat["工艺单元"].astype(str).str.contains("加药间")
    ]

    return {"well_ozone": well_ozone, "hypo": hypo}


def _period_suffix(time_type: int) -> str:
    """timeType -> ‘日/周/月/年’ 后缀"""
    if time_type not in TIME_MAP:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")
    return TIME_MAP[time_type]


# ========= 1) 厂内-预处理段-碳排信息 =========

@router.get("/api/process/inner/预处理/info")
def pretreat_info(
    timeType: int = Query(
        4, description="1=日, 2=周, 3=月, 4=年（默认按年）"
    )
) -> Dict[str, Any]:
    """
    预处理段碳排信息（数据来源：水厂内_分单元）：
    - totalCarbonEmissions:
        工艺段 = 预处理段 的所有单元，在合计_日/周/月/年 中的合计
    - distributionWellPreOzoneContactTankCE:
        工艺段 = 预处理段，工艺单元 = 配水井和预臭氧接触池 的合计
    - dosingRoomCE:
        工艺段 = 预处理段，工艺单元 = 加药间 的合计
    """
    if timeType not in TIME_COL_MAP:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    df = load_unit_table()
    col = TIME_COL_MAP[timeType]

    # 只看预处理段
    pre = df[df["工艺段"] == "预处理段"].copy()
    if pre.empty:
        raise HTTPException(status_code=404, detail="Excel 未找到工艺段：预处理段")

    # 预处理段总碳排（所有单元求和）
    total = float(pre[col].sum())

    # 配水井和预臭氧接触池 —— 直接取这一行的值
    well_row = pre[pre["工艺单元"] == "配水井和预臭氧接触池"]
    if not well_row.empty:
        distribution_well_ozone = float(well_row[col].iloc[0])
    else:
        distribution_well_ozone = 0.0

    # 加药间 —— 直接取这一行的值
    dosing_row = pre[pre["工艺单元"] == "加药间"]
    if not dosing_row.empty:
        dosing_room = float(dosing_row[col].iloc[0])
    else:
        dosing_room = 0.0

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "totalCarbonEmissions": total,
            "distributionWellPreOzoneContactTankCE": distribution_well_ozone,
            "dosingRoomCE": dosing_room,
        },
    })


# ========= 2) 厂内-预处理段-碳排趋势 =========

@router.post("/api/process/inner/预处理/trend")
def pretreat_trend(body: TrendBody) -> Dict[str, Any]:
    """
    qtype:
      1 = 配水井+预臭氧接触池
      2 = 加药间
    timeType:
      1=日，2=周、3=月、4=年

    Excel 目前是“日/周/月/年”的汇总值，没有逐日序列，
    这里按选定周期返回一组数据点：
      xAxis: [ "日" / "周" / "月" / "年" ]
      series:
        - 总碳排
        - 电耗碳排
        - 药耗碳排
    """
    suffix = _period_suffix(body.timeType)  # 日/周/月/年
    unit_dict = _select_pretreat_units()

    if body.qtype == 1:
        target_df = unit_dict["well_ozone"]
        dev_name = "配水井和预臭氧接触池"
    elif body.qtype == 2:
        target_df = unit_dict["hypo"]
        dev_name = "加药间"
    else:
        raise HTTPException(status_code=400, detail="qtype 只能是 1(配水井+预臭氧) 或 2(加药间)")

    if target_df.empty:
        raise HTTPException(status_code=404, detail=f"未在水厂内_分单元中找到 {dev_name} 的数据")

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

    return format_float_2d({
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
    })


# ========= 3) 厂内-预处理段-碳排占比 =========

@router.get("/api/process/inner/预处理/share")
def pretreat_share(timeType: int = 4) -> Dict[str, Any]:
    """
    预处理段内部构成占比（使用《水厂内_分单元》中“段内占比_日/周/月/年”列）

    timeType:
      1 = 日 -> 段内占比_日
      2 = 周 -> 段内占比_周
      3 = 月 -> 段内占比_月
      4 = 年 -> 段内占比_年（默认）
    """
    suffix = _period_suffix(timeType)          # 日 / 周 / 月 / 年
    col_ratio = f"段内占比_{suffix}"

    df = load_unit_table()

    # 只看预处理段
    pretreat = df[df["工艺段"] == "预处理段"].copy()
    if pretreat.empty:
        raise HTTPException(status_code=500, detail="未在水厂内_分单元中找到“预处理段”记录")

    if col_ratio not in pretreat.columns:
        raise HTTPException(status_code=500, detail=f"水厂内_分单元 缺少字段：{col_ratio}")

    well_ozone = pretreat[
        pretreat["工艺单元"].astype(str).str.contains("配水井和预臭氧接触池")
    ]
    hypo = pretreat[
        pretreat["工艺单元"].astype(str).str.contains("加药间")
    ]

    def _sum_ratio(d: pd.DataFrame) -> float:
        if d.empty:
            return 0.0
        return float(pd.to_numeric(d[col_ratio], errors="coerce").fillna(0.0).sum())

    well_ozone_ratio = _sum_ratio(well_ozone)
    hypo_ratio = _sum_ratio(hypo)

    source = [
        {"碳排结构": "配水井和预臭氧接触池", "数据值": well_ozone_ratio},
        {"碳排结构": "加药间", "数据值": hypo_ratio},
    ]

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "dimensions": ["碳排结构", "数据值"],
            "source": source,
            "dimensionsMapping": ["碳排结构", "数据值"],
        },
    })
