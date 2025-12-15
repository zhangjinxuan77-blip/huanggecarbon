# -*- coding: utf-8 -*-
"""
厂内-深度处理段相关接口
- 深度处理段碳排信息       /api/process/inner/深度处理/info          (GET)
- 深度处理段碳排趋势       /api/process/inner/深度处理/trend         (POST)
- 深度处理段碳排占比       /api/process/inner/深度处理/share         (GET)

Excel：data/范围2_水厂内外_分段与单元.xlsx
sheet：
  - 水厂内_分段   => 按工艺段汇总（trend 用它）
  - 水厂内_分单元 => 按工艺单元汇总（info/share 用它）
"""

import os
from typing import Optional, Dict, Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from modules.common import format_float_2d

router = APIRouter()


# ========= 入参模型 =========

class TrendBody(BaseModel):
    # 1=臭氧车间, 2=主臭氧接触池, 3=翻板炭滤池, 4=次氯酸钠投加间
    qtype: int
    # 1=日，2=周、3=月、4=年
    timeType: int


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

# timeType -> 合计列名
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

    # 数值列转 float
    for prefix in ["合计", "电耗", "药耗"]:
        for suff in ["日", "周", "月", "年"]:
            col = f"{prefix}_{suff}"
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

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


def _select_deep_units() -> pd.DataFrame:
    """从《水厂内_分单元》中筛选深度处理段的所有单元"""
    df = load_unit_table()
    deep = df[df["工艺段"] == "深度处理段"].copy()
    if deep.empty:
        raise HTTPException(status_code=500, detail="未在水厂内_分单元中找到“深度处理段”记录")
    return deep


# ========= 1) 厂内-深度处理段-碳排信息 =========

@router.get("/api/process/inner/深度处理/info")
def deep_info(
    timeType: int = Query(
        4, description="1=日, 2=周, 3=月, 4=年（默认按年）"
    )
) -> Dict[str, Any]:
    """
    深度处理段碳排信息（数据来源：水厂内_分单元）：
    - totalCarbonEmissions:
        工艺段 = 深度处理段 的所有单元，在合计_日/周/月/年中的合计
    - ozoneWorkshopCE:
        工艺单元包含 “臭氧车间” 或 “臭氧溶解/混合” 的合计
    - mainOzoneContactTankCE:
        工艺单元包含 “臭氧接触池” 的合计
    - flipPlateCarbonFilterCE:
        工艺单元 = 翻板炭滤池 的合计
    - sodiumHypochloriteDosingRoomCE:
        工艺单元 = 次氯酸钠投加间 的合计
    """
    if timeType not in TIME_COL_MAP:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    df = _select_deep_units()
    col = TIME_COL_MAP[timeType]

    # 段总碳排
    total = float(df[col].sum())

    # 臭氧车间（含“臭氧溶解/混合”）
    ozone_workshop = df[
        df["工艺单元"].astype(str).str.contains("臭氧车间|臭氧溶解/混合", regex=True)
    ]
    ozone_workshop_ce = float(ozone_workshop[col].sum())

    # 主臭氧接触池
    main_ozone = df[df["工艺单元"].astype(str).str.contains("臭氧接触池")]
    main_ozone_ce = float(main_ozone[col].sum())

    # 翻板炭滤池
    flip_filter = df[df["工艺单元"] == "翻板炭滤池"]
    flip_filter_ce = float(flip_filter[col].sum())

    # 次氯酸钠投加间
    sodium_room = df[df["工艺单元"] == "次氯酸钠投加间"]
    sodium_room_ce = float(sodium_room[col].sum())

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "totalCarbonEmissions": total,
            "ozoneWorkshopCE": ozone_workshop_ce,
            "mainOzoneContactTankCE": main_ozone_ce,
            "flipPlateCarbonFilterCE": flip_filter_ce,
            "sodiumHypochloriteDosingRoomCE": sodium_room_ce,
        },
    })


# ========= 2) 厂内-深度处理段-碳排趋势 =========

@router.post("/api/process/inner/深度处理/trend")
def deep_trend(body: TrendBody) -> Dict[str, Any]:
    """
    qtype:
      1 = 臭氧车间
      2 = 主臭氧接触池
      3 = 翻板炭滤池
      4 = 次氯酸钠投加间
    timeType:
      1=日，2=周、3=月、4=年

    数据来源：sheet《水厂内_分单元》
      - 总碳排: 合计_*
      - 电耗碳排: 电耗_*
      - 药耗碳排: 药耗_*
    """
    suffix = _period_suffix(body.timeType)  # 日/周/月/年
    col_total = f"合计_{suffix}"
    col_elec = f"电耗_{suffix}"
    col_chem = f"药耗_{suffix}"

    df = _select_deep_units()

    # 选定目标单元
    if body.qtype == 1:
        name = "臭氧车间"
        target = df[df["工艺单元"].astype(str).str.contains("臭氧车间|臭氧溶解/混合", regex=True)]
    elif body.qtype == 2:
        name = "主臭氧接触池"
        target = df[df["工艺单元"].astype(str).str.contains("臭氧接触池")]
    elif body.qtype == 3:
        name = "翻板炭滤池"
        target = df[df["工艺单元"] == "翻板炭滤池"]
    elif body.qtype == 4:
        name = "次氯酸钠投加间"
        target = df[df["工艺单元"] == "次氯酸钠投加间"]
    else:
        raise HTTPException(
            status_code=400,
            detail="qtype 只能是 1(臭氧车间)/2(主臭氧接触池)/3(翻板炭滤池)/4(次氯酸钠投加间)",
        )

    if target.empty:
        raise HTTPException(status_code=404, detail=f"未在水厂内_分单元中找到 {name} 的数据")

    for c in [col_total, col_elec, col_chem]:
        if c not in target.columns:
            raise HTTPException(status_code=500, detail=f"水厂内_分单元 缺少字段：{c}")

    total_val = float(target[col_total].sum())
    elec_val = float(target[col_elec].sum())
    chem_val = float(target[col_chem].sum())

    period_label = suffix  # “日/周/月/年” 作为 x 轴标签

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


# ========= 3) 厂内-深度处理段-碳排占比 =========

@router.get("/api/process/inner/深度处理/share")
def deep_share(timeType: int = 4) -> Dict[str, Any]:
    """
    深度处理段内部构成占比（使用《水厂内_分单元》中“段内占比_日/周/月/年”列）

    timeType:
      1 = 日 -> 段内占比_日
      2 = 周 -> 段内占比_周
      3 = 月 -> 段内占比_月
      4 = 年 -> 段内占比_年（默认）

    维度：
      - 臭氧车间碳排（含“臭氧溶解/混合”）
      - 主臭氧接触池碳排
      - 翻板炭滤池碳排
      - 次氯酸钠投加间碳排
    """
    suffix = _period_suffix(timeType)
    col_ratio = f"段内占比_{suffix}"

    df = _select_deep_units()

    if col_ratio not in df.columns:
        raise HTTPException(status_code=500, detail=f"水厂内_分单元 缺少字段：{col_ratio}")

    # 各单元
    ozone_workshop = df[
        df["工艺单元"].astype(str).str.contains("臭氧车间|臭氧溶解/混合", regex=True)
    ]
    main_ozone = df[df["工艺单元"].astype(str).str.contains("臭氧接触池")]
    flip_filter = df[df["工艺单元"] == "翻板炭滤池"]
    sodium_room = df[df["工艺单元"] == "次氯酸钠投加间"]

    def _sum_ratio(d: pd.DataFrame) -> float:
        if d.empty:
            return 0.0
        return float(pd.to_numeric(d[col_ratio], errors="coerce").fillna(0.0).sum())

    ozone_workshop_ratio = _sum_ratio(ozone_workshop)
    main_ozone_ratio = _sum_ratio(main_ozone)
    flip_filter_ratio = _sum_ratio(flip_filter)
    sodium_room_ratio = _sum_ratio(sodium_room)

    source = [
        {"工艺单元": "臭氧车间碳排", "数据值": ozone_workshop_ratio},
        {"工艺单元": "主臭氧接触池碳排", "数据值": main_ozone_ratio},
        {"工艺单元": "翻板炭滤池碳排", "数据值": flip_filter_ratio},
        {"工艺单元": "次氯酸钠投加间碳排", "数据值": sodium_room_ratio},
    ]

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "dimensions": ["工艺单元", "数据值"],
            "source": source,
            "dimensionsMapping": ["工艺单元", "数据值"],
        },
    })
