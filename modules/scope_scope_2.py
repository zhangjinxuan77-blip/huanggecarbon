# -*- coding: utf-8 -*-
"""
范围2接口  /api/scope/scope_2

入参：
  {
    "timeType": 1  # 1=日, 2=周, 3=月, 4=年
  }

数据来源：
  1) 碳排总汇总.xlsx
       - sheet: 总汇总
       - 字段：周期、范围2(kgCO2e)
       - 周期: 日 / 周 / 月 / 年

  2) 范围2_水厂内外_分段与单元.xlsx
       - sheet: 水厂内_分段   (全厂电耗碳排 = 5 个工艺段电耗_* 之和)
       - sheet: 水厂外_分段   (厂外电耗碳排、取水段/送水段电耗碳排)
       - sheet: 用电量_kWh    (取水段/送水段用电量 kWh_*)

  3) 全厂总用电量：固定常数 3,272,523 kWh（年）
       - 日/周/月口径按年值简单折算：
           日 = 年/365
           周 = 年/52
           月 = 年/12
           年 = 年
"""

import os
from typing import Optional, Dict, Any
from modules.common import format_float_2d
import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()


# ========= 入参模型 =========

class Scope2Body(BaseModel):
    timeType: int   # 1=日, 2=周, 3=月, 4=年


# ========= 配置与缓存 =========

# Excel 路径（按你的项目 data 目录来）
SCOPE2_XLSX = os.path.join("data", "范围2_水厂内外_分段与单元.xlsx")
TOTAL_XLSX  = os.path.join("data", "碳排总汇总.xlsx")

# timeType -> “日/周/月/年”
TIME_LABEL_MAP: Dict[int, str] = {
    1: "日",
    2: "周",
    3: "月",
    4: "年",
}

# timeType -> 电耗列名 / kWh 列名
ELEC_CARBON_COL_MAP: Dict[int, str] = {
    1: "电耗_日",
    2: "电耗_周",
    3: "电耗_月",
    4: "电耗_年",
}
ELEC_KWH_COL_MAP: Dict[int, str] = {
    1: "kWh_日",
    2: "kWh_周",
    3: "kWh_月",
    4: "kWh_年",
}

# 全厂年总用电量（kWh）
TOTAL_ELEC_KWH_YEAR: float = 3_272_523.0

# 缓存
_TOTAL_TABLE: Optional[pd.DataFrame]      = None  # 碳排总汇总.xlsx / 总汇总
_INNER_SECTION_TABLE: Optional[pd.DataFrame] = None  # 范围2 / 水厂内_分段
_OUTER_SECTION_TABLE: Optional[pd.DataFrame] = None  # 范围2 / 水厂外_分段
_ELEC_TABLE: Optional[pd.DataFrame]       = None  # 范围2 / 用电量_kWh


# ========= 读表工具函数 =========

def load_total_table() -> pd.DataFrame:
    """读取《碳排总汇总.xlsx》- 总汇总（仅加载一次）"""
    global _TOTAL_TABLE
    if _TOTAL_TABLE is not None:
        return _TOTAL_TABLE

    try:
        df = pd.read_excel(TOTAL_XLSX, sheet_name="总汇总")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"碳排总汇总.xlsx(总汇总) 加载失败: {e}")

    # 必需字段：周期、范围2(kgCO2e)
    for col in ["周期", "范围2(kgCO2e)"]:
        if col not in df.columns:
            raise HTTPException(status_code=500, detail=f"碳排总汇总·总汇总 缺少字段：{col}")

    df["周期"] = df["周期"].astype(str).str.strip()
    df["范围2(kgCO2e)"] = pd.to_numeric(df["范围2(kgCO2e)"], errors="coerce").fillna(0.0)

    _TOTAL_TABLE = df
    return df


def load_inner_section_table() -> pd.DataFrame:
    """读取《范围2_水厂内外_分段与单元.xlsx》- 水厂内_分段"""
    global _INNER_SECTION_TABLE
    if _INNER_SECTION_TABLE is not None:
        return _INNER_SECTION_TABLE

    try:
        df = pd.read_excel(SCOPE2_XLSX, sheet_name="水厂内_分段")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"范围2(水厂内_分段) 加载失败: {e}")

    if "工艺段" not in df.columns:
        raise HTTPException(status_code=500, detail="水厂内_分段 缺少字段：工艺段")

    for col in ELEC_CARBON_COL_MAP.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    _INNER_SECTION_TABLE = df
    return df


def load_outer_section_table() -> pd.DataFrame:
    """读取《范围2_水厂内外_分段与单元.xlsx》- 水厂外_分段"""
    global _OUTER_SECTION_TABLE
    if _OUTER_SECTION_TABLE is not None:
        return _OUTER_SECTION_TABLE

    try:
        df = pd.read_excel(SCOPE2_XLSX, sheet_name="水厂外_分段")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"范围2(水厂外_分段) 加载失败: {e}")

    if "工艺段" not in df.columns:
        raise HTTPException(status_code=500, detail="水厂外_分段 缺少字段：工艺段")

    for col in ELEC_CARBON_COL_MAP.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    _OUTER_SECTION_TABLE = df
    return df


def load_elec_table() -> pd.DataFrame:
    """读取《范围2_水厂内外_分段与单元.xlsx》- 用电量_kWh"""
    global _ELEC_TABLE
    if _ELEC_TABLE is not None:
        return _ELEC_TABLE

    try:
        df = pd.read_excel(SCOPE2_XLSX, sheet_name="用电量_kWh")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"范围2(用电量_kWh) 加载失败: {e}")

    if "工艺段" not in df.columns:
        raise HTTPException(status_code=500, detail="用电量_kWh 缺少字段：工艺段")

    for col in ELEC_KWH_COL_MAP.values():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    _ELEC_TABLE = df
    return df


# ========= 公共小函数 =========

def _validate_time_type(time_type: int) -> None:
    if time_type not in TIME_LABEL_MAP:
        raise HTTPException(
            status_code=400,
            detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)",
        )


def _total_electricity_by_time(time_type: int) -> float:
    """根据年总用电量折算 日/周/月/年"""
    if time_type == 4:   # 年
        return float(TOTAL_ELEC_KWH_YEAR)
    if time_type == 1:   # 日
        return float(TOTAL_ELEC_KWH_YEAR / 365.0)
    if time_type == 2:   # 周
        return float(TOTAL_ELEC_KWH_YEAR / 52.0)
    if time_type == 3:   # 月
        return float(TOTAL_ELEC_KWH_YEAR / 12.0)
    return float(TOTAL_ELEC_KWH_YEAR)


# ========= 范围2接口 =========

@router.post("/api/scope/scope_2")
def scope_2(body: Scope2Body) -> Dict[str, Any]:
    """
    范围2汇总接口：
      - totalCarbonEmissions: 范围2总碳排放量（碳排总汇总.xlsx / 总汇总 / 范围2(kgCO2e)）
      - totalPlantElectricityConsumptionCarbonEmissions: 全厂电耗碳排量（范围2 / 水厂内_分段，5个工艺段电耗_* 之和）
      - totalElectricityConsumption: 全厂总用电量（3,272,523 kWh 按日周月年折算）
      - offSiteElectricityConsumptionCarbonEmissions: 厂外电耗碳排量（范围2 / 水厂外_分段，原水取水段+供水段）
      - qsdElectricityConsumption / qsdElectricityConsumptionCarbonEmissions:
            取水段(原水取水段) 用电量 / 电耗碳排
      - ssdElectricityConsumption / ssdElectricityConsumptionCarbonEmissions:
            送水段(供水段) 用电量 / 电耗碳排
    """
    time_type = body.timeType
    _validate_time_type(time_type)

    period_label = TIME_LABEL_MAP[time_type]          # 日/周/月/年
    carbon_col = ELEC_CARBON_COL_MAP[time_type]       # 电耗_日/周/月/年
    kwh_col    = ELEC_KWH_COL_MAP[time_type]          # kWh_日/周/月/年

    # ----- 1) 范围2总碳排量（碳排总汇总.xlsx -> 总汇总） -----
    total_df = load_total_table()
    row = total_df[total_df["周期"] == period_label]
    if row.empty:
        total_scope2 = 0.0
    else:
        total_scope2 = float(row["范围2(kgCO2e)"].sum())

    # ----- 2) 全厂电耗碳排量（范围2 / 水厂内_分段） -----
    inner_df = load_inner_section_table()
    total_plant_elec_ce = float(inner_df[carbon_col].sum())

    # ----- 3) 全厂总用电量（固定常数折算） -----
    total_elec_kwh = _total_electricity_by_time(time_type)

    # ----- 4) 厂外电耗碳排量 + 取水/送水电耗碳排（范围2 / 水厂外_分段） -----
    outer_df = load_outer_section_table()

    # 工艺段名按你表里的写法：原水取水段、供水段
    qsd_sec = outer_df[outer_df["工艺段"] == "原水取水段"]
    ssd_sec = outer_df[outer_df["工艺段"] == "供水段"]

    qsd_elec_ce = float(qsd_sec[carbon_col].sum()) if not qsd_sec.empty else 0.0
    ssd_elec_ce = float(ssd_sec[carbon_col].sum()) if not ssd_sec.empty else 0.0

    offsite_elec_ce = qsd_elec_ce + ssd_elec_ce

    # ----- 5) 取水/送水用电量（范围2 / 用电量_kWh） -----
    elec_df = load_elec_table()

    # 只取 “层级 = 分段”
    seg_df = elec_df[elec_df["层级"] == "分段"]

    qsd_elec_kwh_df = seg_df[seg_df["工艺段"] == "原水取水段"]
    ssd_elec_kwh_df = seg_df[seg_df["工艺段"] == "供水段"]

    qsd_elec_kwh = float(qsd_elec_kwh_df[kwh_col].sum()) if not qsd_elec_kwh_df.empty else 0.0
    ssd_elec_kwh = float(ssd_elec_kwh_df[kwh_col].sum()) if not ssd_elec_kwh_df.empty else 0.0


    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "totalCarbonEmissions": total_scope2,
            "totalPlantElectricityConsumptionCarbonEmissions": total_plant_elec_ce,
            "totalElectricityConsumption": total_elec_kwh,
            "offSiteElectricityConsumptionCarbonEmissions": offsite_elec_ce,
            "qsdElectricityConsumption": qsd_elec_kwh,
            "qsdElectricityConsumptionCarbonEmissions": qsd_elec_ce,
            "ssdElectricityConsumption": ssd_elec_kwh,
            "ssdElectricityConsumptionCarbonEmissions": ssd_elec_ce,
        },
    })
