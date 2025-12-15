# -*- coding: utf-8 -*-
"""
场外-原水取水段相关接口

- 原水取水段碳排信息   /api/process/outer/原水取水/info   (GET)
- 原水取水段碳排趋势   /api/process/outer/原水取水/trend  (POST)
- 原水取水段碳排占比   /api/process/outer/原水取水/share  (GET)

Excel：data/范围2_水厂内外_分段与单元.xlsx
sheet：
  - 水厂外_分段   => 按工艺段汇总（trend 用）
  - 水厂外_分单元 => 按工艺单元汇总（info/share 用）
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
    # 1=原水提升泵房, 2=取水泵站, 3=次氯酸钠间
    qtype: int
    # 1=日，2=周、3=月、4=年
    timeType: int

# ========= 全局缓存 =========

_SECTION_TABLE: Optional[pd.DataFrame] = None     # 水厂外_分段
_UNIT_TABLE: Optional[pd.DataFrame] = None        # 水厂外_分单元

EXCEL_PATH = os.path.join("data", "范围2_水厂内外_分段与单元.xlsx")

# 工艺段名称
SEG_NAME = "原水取水段"

# timeType -> “日/周/月/年”
TIME_MAP = {1: "日", 2: "周", 3: "月", 4: "年"}
TIME_COL_MAP = {
    1: "合计_日",
    2: "合计_周",
    3: "合计_月",
    4: "合计_年",
}

# ========= 加载表格 =========

def load_section_table() -> pd.DataFrame:
    global _SECTION_TABLE
    if _SECTION_TABLE is not None:
        return _SECTION_TABLE

    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name="水厂外_分段")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Excel(水厂外_分段)加载失败: {e}")

    for prefix in ["合计", "电耗", "药耗"]:
        for suff in ["日", "周", "月", "年"]:
            col = f"{prefix}_{suff}"
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    _SECTION_TABLE = df
    return df


def load_unit_table() -> pd.DataFrame:
    global _UNIT_TABLE
    if _UNIT_TABLE is not None:
        return _UNIT_TABLE

    try:
        df = pd.read_excel(EXCEL_PATH, sheet_name="水厂外_分单元")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Excel(水厂外_分单元)加载失败: {e}")

    for prefix in ["合计", "电耗", "药耗", "段内占比"]:
        for suff in ["日", "周", "月", "年"]:
            col = f"{prefix}_{suff}"
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    _UNIT_TABLE = df
    return df


# ========= 工具函数 =========

def _period_suffix(time_type: int) -> str:
    if time_type not in TIME_MAP:
        raise HTTPException(status_code=400, detail="timeType 必须是 1(日) / 2(周) / 3(月) / 4(年)")
    return TIME_MAP[time_type]

def _select_units():
    df = load_unit_table()
    seg = df[df["工艺段"] == SEG_NAME].copy()
    if seg.empty:
        raise HTTPException(status_code=500, detail=f"未在 水厂外_分单元 中找到“{SEG_NAME}”")
    return seg

def _select_section():
    df = load_section_table()
    seg = df[df["工艺段"] == SEG_NAME].copy()
    if seg.empty:
        raise HTTPException(status_code=500, detail=f"未在 水厂外_分段 中找到“{SEG_NAME}”")
    return seg


# ========= 1) 场外-原水取水段-碳排信息 =========

@router.get("/api/process/outer/原水取水/info")
def rawwater_info(
    timeType: int = Query(4, description="1=日,2=周,3=月,4=年（默认年）")
) -> Dict[str, Any]:

    df = _select_units()
    col = TIME_COL_MAP[timeType]

    total = float(df[col].sum())

    lift = df[df["工艺单元"].astype(str).str.contains("原水提升")]
    station = df[df["工艺单元"].astype(str).str.contains("取水泵站")]
    hypo = df[df["工艺单元"].astype(str).str.contains("次氯酸钠")]

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "totalCarbonEmissions": float(df[col].sum()),
            "rawWaterLiftingPumpHouseCE": float(lift[col].sum()),
            "waterIntakePumpingStationCE": float(station[col].sum()),
            "sodiumHypochloriteRoomCE": float(hypo[col].sum()),
        }
    })


# ========= 2) 场外-原水取水段-碳排趋势 =========

@router.post("/api/process/outer/原水取水/trend")
def rawwater_trend(body: TrendBody) -> Dict[str, Any]:

    suffix = _period_suffix(body.timeType)
    col_total = f"合计_{suffix}"
    col_elec = f"电耗_{suffix}"
    col_chem = f"药耗_{suffix}"

    df = _select_units()

    # 选择单元
    if body.qtype == 1:
        name = "原水提升泵房"
        target = df[df["工艺单元"].str.contains("原水提升")]
    elif body.qtype == 2:
        name = "取水泵站"
        target = df[df["工艺单元"].str.contains("取水泵站")]
    elif body.qtype == 3:
        name = "次氯酸钠间"
        target = df[df["工艺单元"].str.contains("次氯酸钠")]
    else:
        raise HTTPException(status_code=400, detail="qtype 必须是 1/2/3")

    if target.empty:
        raise HTTPException(status_code=404, detail=f"未找到 {name} 对应数据")

    total_val = float(target[col_total].sum())
    elec_val = float(target[col_elec].sum())
    chem_val = float(target[col_chem].sum())

    # x 轴名称（你可按前端需要改）
    period_label = suffix

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "id": "80",
            "styleType": "0",
            "customOption": {},
            "xAxis": [{"type": "category", "name": "", "data": [period_label]}],
            "yAxis": [{"name": "kgCO₂e", "type": "value"}],
            "series": [
                {"name": "总碳排", "type": "line", "data": [total_val]},
                {"name": f"{name}电耗碳排", "type": "line", "data": [elec_val]},
                {"name": f"{name}药耗碳排", "type": "line", "data": [chem_val]},
            ],
            "colors": [
                "#4992ff", "#7cffb2", "#dd79ff", "#fddd60", "#ff6e76",
                "#58d9f9", "#05c091", "#ff8a45", "#8d48e3"
            ],
        },
    })


# ========= 3) 场外-原水取水段-碳排占比 =========

@router.get("/api/process/outer/原水取水/share")
def rawwater_share(timeType: int = 4) -> Dict[str, Any]:

    suffix = _period_suffix(timeType)
    col_ratio = f"段内占比_{suffix}"

    df = _select_units()

    lift = df[df["工艺单元"].str.contains("原水提升")]
    station = df[df["工艺单元"].str.contains("取水泵站")]
    hypo = df[df["工艺单元"].str.contains("次氯酸钠")]

    def _sum_ratio(d):
        return float(pd.to_numeric(d[col_ratio], errors="coerce").fillna(0).sum())

    source = [
        {"碳排结构": "原水提升泵房", "数据值": _sum_ratio(lift)},
        {"碳排结构": "取水泵站", "数据值": _sum_ratio(station)},
        {"碳排结构": "次氯酸钠间", "数据值": _sum_ratio(hypo)},
    ]

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "dimensions": ["碳排结构", "数据值"],
            "source": source,
            "dimensionsMapping": ["product", "数据值"],
        },
    })
