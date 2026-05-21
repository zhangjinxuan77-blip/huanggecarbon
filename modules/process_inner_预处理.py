# -*- coding: utf-8 -*-

import os
from typing import Any, Dict

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel


router = APIRouter()


class TrendBody(BaseModel):
    qtype: int
    timeType: int


class TimeBody(BaseModel):
    timeType: int = 4


BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(
    BASE_DIR,
    "data",
    "real-time output",
    "process_stage_outputs",
    "03_预处理",
)

CARBON_UNIT = "kgCO2e"
SHARE_UNIT = "%"

TIME_CONFIG = {
    1: {"label": "日", "summary_period": "latest_24h_hourly", "trend_file": "latest_24h_hourly.csv"},
    2: {"label": "周", "summary_period": "latest_7d_daily", "trend_file": "latest_7d_daily.csv"},
    3: {"label": "月", "summary_period": "latest_5w_weekly", "trend_file": "latest_5w_weekly.csv"},
    4: {"label": "年", "summary_period": "latest_12m_monthly", "trend_file": "latest_12m_monthly.csv"},
}

UNIT_MAP = {
    1: "预处理_配水井和预臭氧接触池",
    2: "预处理_加药间",
}

DISPLAY_UNIT_MAP = {
    "预处理_配水井和预臭氧接触池": "配水井和预臭氧接触池",
    "预处理_加药间": "加药间",
}


def _sig2(value: Any) -> float:
    """保留两位有效数字。"""
    try:
        return float(f"{float(value):.2g}")
    except Exception:
        return 0.0


def _fmt(value: Any, unit: str) -> str:
    return f"{_sig2(value):g} {unit}" if unit != SHARE_UNIT else f"{_sig2(value):g}{unit}"


def _round_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _round_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_round_obj(v) for v in obj]
    if isinstance(obj, float):
        return _sig2(obj)
    return obj


def _time_config(time_type: int) -> dict:
    config = TIME_CONFIG.get(time_type)
    if not config:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")
    return config


def _read_csv(filename: str) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=500, detail=f"文件不存在：{path}")

    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"CSV 读取失败：{path}，{exc}")

    if df.empty:
        raise HTTPException(status_code=404, detail=f"CSV 没有数据：{path}")

    return df


def _ensure_columns(df: pd.DataFrame, columns: list[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise HTTPException(status_code=500, detail=f"CSV 缺少字段：{missing}")


def _summary_rows(time_type: int) -> pd.DataFrame:
    config = _time_config(time_type)
    df = _read_csv("summary.csv")
    required = [
        "summary_period",
        "summary_level",
        "process_unit",
        "electric_carbon_kg_sum",
        "chemical_carbon_kg_sum",
        "unit_total_carbon_kg_sum",
        "unit_share_within_stage_avg",
    ]
    _ensure_columns(df, required)

    rows = df[df["summary_period"] == config["summary_period"]].copy()
    if rows.empty:
        raise HTTPException(status_code=404, detail=f"summary.csv 未找到：{config['summary_period']}")

    numeric_cols = [
        "electric_carbon_kg_sum",
        "chemical_carbon_kg_sum",
        "unit_total_carbon_kg_sum",
        "unit_share_within_stage_avg",
    ]
    for col in numeric_cols:
        rows[col] = pd.to_numeric(rows[col], errors="coerce").fillna(0.0)

    return rows


def _summary_detail_rows(time_type: int) -> pd.DataFrame:
    rows = _summary_rows(time_type)
    return rows[rows["summary_level"] == "detail"].copy()


def _summary_total_row(time_type: int) -> pd.Series:
    rows = _summary_rows(time_type)
    total_rows = rows[rows["summary_level"] == "total"]
    if total_rows.empty:
        raise HTTPException(status_code=404, detail="summary.csv 未找到 total 汇总行")
    return total_rows.iloc[0]


def _trend_table(time_type: int) -> pd.DataFrame:
    config = _time_config(time_type)
    df = _read_csv(config["trend_file"])
    required = [
        "period_start",
        "process_unit",
        "electric_carbon_kg",
        "chemical_carbon_kg",
        "unit_total_carbon_kg",
    ]
    _ensure_columns(df, required)

    df = df.copy()
    df["period_start"] = pd.to_datetime(df["period_start"], errors="coerce")
    df = df.sort_values("period_start")
    for col in ["electric_carbon_kg", "chemical_carbon_kg", "unit_total_carbon_kg"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def _format_time(value: Any, time_type: int) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return str(value)
    if time_type == 1:
        return dt.strftime("%H:%M")
    if time_type in (2, 3):
        return dt.strftime("%m-%d")
    return f"{dt.month}月"


def _pretreat_info_payload(timeType: int) -> Dict[str, Any]:
    config = _time_config(timeType)
    total = _summary_total_row(timeType)
    details = _summary_detail_rows(timeType)

    def value_for(unit_name: str) -> float:
        row = details[details["process_unit"] == unit_name]
        if row.empty:
            return 0.0
        return float(row.iloc[0]["unit_total_carbon_kg_sum"])

    total_value = float(total["unit_total_carbon_kg_sum"])
    well_value = value_for("预处理_配水井和预臭氧接触池")
    dosing_value = value_for("预处理_加药间")

    result = {
        "code": 0,
        "msg": "",
        "data": {
            "unit": CARBON_UNIT,
            "totalCarbonEmissions": total_value,
            "totalCarbonEmissionsWithUnit": _fmt(total_value, CARBON_UNIT),
            "distributionWellPreOzoneContactTankCE": well_value,
            "distributionWellPreOzoneContactTankCEWithUnit": _fmt(well_value, CARBON_UNIT),
            "sodiumHypochloriteRoomCE": dosing_value,
            "sodiumHypochloriteRoomCEWithUnit": _fmt(dosing_value, CARBON_UNIT),
        },
    }
    return _round_obj(result)


@router.get("/api/process/inner/预处理/info")
def pretreat_info(timeType: int = Query(4)) -> Dict[str, Any]:
    return _pretreat_info_payload(timeType)


@router.post("/api/process/inner/预处理/info")
def pretreat_info_post(body: TimeBody) -> Dict[str, Any]:
    return _pretreat_info_payload(int(body.timeType))


@router.post("/api/process/inner/预处理/trend")
def pretreat_trend(body: TrendBody) -> Dict[str, Any]:
    config = _time_config(body.timeType)
    if body.qtype not in UNIT_MAP:
        raise HTTPException(status_code=400, detail="qtype 只能是 1 或 2")

    unit_name = UNIT_MAP[body.qtype]
    target = _trend_table(body.timeType)
    target = target[target["process_unit"] == unit_name].copy()

    if target.empty:
        raise HTTPException(status_code=404, detail=f"未找到工艺单元：{unit_name}")

    x_data = [_format_time(v, body.timeType) for v in target["period_start"].tolist()]
    total_data = target["unit_total_carbon_kg"].tolist()
    electric_data = target["electric_carbon_kg"].tolist()
    chemical_data = target["chemical_carbon_kg"].tolist()

    result = {
        "code": 0,
        "msg": "",
        "data": {
            "id": "80",
            "styleType": "0",
            "customOption": {},
            "xAxis": [{
                "type": "category",
                "name": "",
                "data": x_data,
            }],
            "yAxis": [{
                "name": CARBON_UNIT,
                "type": "value",
            }],
            "series": [
                {
                    "name": "总碳排",
                    "type": "line",
                    "data": total_data,
                },
                {
                    "name": f"{DISPLAY_UNIT_MAP.get(unit_name, unit_name)}电耗碳排",
                    "type": "line",
                    "data": electric_data,
                },
                {
                    "name": f"{DISPLAY_UNIT_MAP.get(unit_name, unit_name)}药耗碳排",
                    "type": "line",
                    "data": chemical_data,
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
    return _round_obj(result)


def _pretreat_share_payload(timeType: int) -> Dict[str, Any]:
    config = _time_config(timeType)
    details = _summary_detail_rows(timeType)

    def share_for(unit_name: str) -> float:
        row = details[details["process_unit"] == unit_name]
        if row.empty:
            return 0.0
        return float(row.iloc[0]["unit_share_within_stage_avg"]) * 100.0

    well_share = share_for("预处理_配水井和预臭氧接触池")
    dosing_share = share_for("预处理_加药间")

    result = {
        "code": 0,
        "msg": "",
        "data": {
            "unit": SHARE_UNIT,
            "dimensions": ["碳排结构", "数据值"],
            "source": [
                {
                    "碳排结构": "配水井和预臭氧接触池",
                    "数据值": well_share,
                },
                {
                    "碳排结构": "加药间",
                    "数据值": dosing_share,
                },
            ],
            "dimensionsMapping": ["碳排结构", "数据值"],
        },
    }
    return _round_obj(result)


@router.get("/api/process/inner/预处理/share")
def pretreat_share(timeType: int = Query(4)) -> Dict[str, Any]:
    return _pretreat_share_payload(timeType)


@router.post("/api/process/inner/预处理/share")
def pretreat_share_post(body: TimeBody = TimeBody()) -> Dict[str, Any]:
    return _pretreat_share_payload(int(body.timeType))
