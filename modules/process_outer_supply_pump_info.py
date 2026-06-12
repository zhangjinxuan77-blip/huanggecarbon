# -*- coding: utf-8 -*-
"""水厂外供水段泵房能耗与碳排静态信息。"""

from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException


router = APIRouter()

BASE_DIR = Path(__file__).resolve().parent.parent
REALTIME_DIR = BASE_DIR / "data" / "real-time output"
PUMP_SUMMARY_FILE = (
    REALTIME_DIR
    / "carbon_outputs"
    / "11_清水处理_送水泵"
    / "summary.csv"
)
INTENSITY_SUMMARY_FILE = (
    REALTIME_DIR
    / "单位水处理强度和减排量"
    / "summary_单位水处理强度和减排量.csv"
)
SUMMARY_PERIOD = "latest_24h_hourly"


def _read_summary(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise HTTPException(status_code=500, detail=f"未找到数据文件：{path}")
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"CSV 读取失败：{path}，{exc}")
    if df.empty:
        raise HTTPException(status_code=404, detail=f"CSV 没有数据：{path}")
    return df


def _period_row(df: pd.DataFrame, period_column: str) -> pd.Series:
    if period_column not in df.columns:
        raise HTTPException(
            status_code=500,
            detail=f"CSV 缺少字段：{period_column}",
        )
    rows = df[df[period_column].astype(str) == SUMMARY_PERIOD]
    if rows.empty:
        raise HTTPException(
            status_code=404,
            detail=f"未找到最近24小时汇总：{SUMMARY_PERIOD}",
        )
    return rows.iloc[0]


def _number(row: pd.Series, column: str) -> float:
    if column not in row.index:
        raise HTTPException(status_code=500, detail=f"CSV 缺少字段：{column}")
    value = pd.to_numeric(row[column], errors="coerce")
    return 0.0 if pd.isna(value) else float(value)


def build_supply_pump_info() -> dict:
    pump_row = _period_row(_read_summary(PUMP_SUMMARY_FILE), "summary_period")
    water_row = _period_row(_read_summary(INTENSITY_SUMMARY_FILE), "summary周期")

    total_energy = _number(pump_row, "unit_total_energy_kWh_sum")
    carbon_emissions = _number(pump_row, "unit_total_carbon_kg_sum")
    water_supply = _number(water_row, "水处理量_m3")
    unit_energy = total_energy / water_supply if water_supply > 0 else 0.0

    return {
        "code": 0,
        "msg": "",
        "data": {
            "totalEnergyConsumption": f"{total_energy:.2f}",
            "waterSupplyFlowRate": f"{water_supply:.2f}",
            "unitEnergyConsumptionforWaterIntake": f"{unit_energy:.6f}",
            "carbonEmissionsPumpHouse": f"{carbon_emissions:.2f}",
        },
    }


@router.get("/api/process/outer/供水段/pump_house_info")
@router.post("/api/process/outer/供水段/pump_house_info")
def supply_pump_info():
    return build_supply_pump_info()
