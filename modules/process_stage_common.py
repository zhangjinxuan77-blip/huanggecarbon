# -*- coding: utf-8 -*-

import os
from typing import Any, Callable, Dict, List

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel


CARBON_UNIT = "kgCO2e"
COLORS = [
    "#4992ff",
    "#7cffb2",
    "#dd79ff",
    "#fddd60",
    "#ff6e76",
    "#58d9f9",
    "#05c091",
    "#ff8a45",
    "#8d48e3",
]

TIME_CONFIG = {
    1: {"label": "日", "summary_period": "latest_24h_hourly", "trend_file": "latest_24h_hourly.csv"},
    2: {"label": "周", "summary_period": "latest_7d_daily", "trend_file": "latest_7d_daily.csv"},
    3: {"label": "月", "summary_period": "latest_5w_weekly", "trend_file": "latest_5w_weekly.csv"},
    4: {"label": "年", "summary_period": "latest_12m_monthly", "trend_file": "latest_12m_monthly.csv"},
}
STATIC_INFO_TIME_TYPE = 1


class TimeBody(BaseModel):
    timeType: int = 4


class TrendBody(BaseModel):
    qtype: int
    timeType: int


def sig2(value: Any) -> float:
    try:
        return float(f"{float(value):.2g}")
    except Exception:
        return 0.0


def round_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: round_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [round_obj(v) for v in obj]
    if isinstance(obj, float):
        return sig2(obj)
    return obj


def time_config(time_type: int) -> dict:
    config = TIME_CONFIG.get(time_type)
    if not config:
        raise HTTPException(status_code=400, detail="timeType 只能是 1(日)/2(周)/3(月)/4(年)")
    return config


def read_csv(data_dir: str, filename: str) -> pd.DataFrame:
    path = os.path.join(data_dir, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=500, detail=f"文件不存在：{path}")
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"CSV 读取失败：{path}，{exc}")
    if df.empty:
        raise HTTPException(status_code=404, detail=f"CSV 没有数据：{path}")
    return df


def ensure_columns(df: pd.DataFrame, columns: List[str]) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise HTTPException(status_code=500, detail=f"CSV 缺少字段：{missing}")


def unit_mask(df: pd.DataFrame, aliases: List[str]) -> pd.Series:
    if not aliases:
        return pd.Series([False] * len(df), index=df.index)
    names = df["process_unit"].astype(str)
    mask = pd.Series([False] * len(df), index=df.index)
    for alias in aliases:
        mask = mask | names.str.contains(alias, regex=False, na=False)
    return mask


def summary_rows(data_dir: str, time_type: int) -> pd.DataFrame:
    config = time_config(time_type)
    df = read_csv(data_dir, "summary.csv")
    required = [
        "summary_period",
        "summary_level",
        "process_unit",
        "electric_carbon_kg_sum",
        "chemical_carbon_kg_sum",
        "unit_total_carbon_kg_sum",
        "unit_share_within_stage_avg",
    ]
    ensure_columns(df, required)
    rows = df[df["summary_period"] == config["summary_period"]].copy()
    if rows.empty:
        raise HTTPException(status_code=404, detail=f"summary.csv 未找到：{config['summary_period']}")
    for col in required[3:]:
        rows[col] = pd.to_numeric(rows[col], errors="coerce").fillna(0.0)
    return rows


def detail_rows(data_dir: str, time_type: int) -> pd.DataFrame:
    return summary_rows(data_dir, time_type).query("summary_level == 'detail'").copy()


def total_row(data_dir: str, time_type: int) -> pd.Series:
    rows = summary_rows(data_dir, time_type).query("summary_level == 'total'")
    if rows.empty:
        raise HTTPException(status_code=404, detail="summary.csv 未找到 total 汇总行")
    return rows.iloc[0]


def sum_for_aliases(rows: pd.DataFrame, aliases: List[str], col: str) -> float:
    if rows.empty:
        return 0.0
    return float(rows[unit_mask(rows, aliases)][col].sum())


def format_time(value: Any, time_type: int) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return str(value)
    if time_type == 1:
        return dt.strftime("%H:%M")
    if time_type in (2, 3):
        return dt.strftime("%m-%d")
    return f"{dt.month}月"


def trend_table(data_dir: str, time_type: int) -> pd.DataFrame:
    config = time_config(time_type)
    df = read_csv(data_dir, config["trend_file"])
    required = [
        "period_start",
        "process_unit",
        "electric_carbon_kg",
        "chemical_carbon_kg",
        "unit_total_carbon_kg",
    ]
    ensure_columns(df, required)
    df = df.copy()
    df["period_start"] = pd.to_datetime(df["period_start"], errors="coerce")
    df = df.sort_values("period_start")
    for col in required[2:]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def aggregate_trend(df: pd.DataFrame, aliases: List[str], time_type: int) -> tuple[list[str], list[float], list[float], list[float]]:
    base_periods = df[["period_start"]].drop_duplicates().sort_values("period_start")
    target = df[unit_mask(df, aliases)].copy()
    if target.empty:
        x_data = [format_time(v, time_type) for v in base_periods["period_start"].tolist()]
        zeros = [0.0] * len(x_data)
        return x_data, zeros, zeros, zeros

    grouped = target.groupby("period_start", as_index=False)[
        ["unit_total_carbon_kg", "electric_carbon_kg", "chemical_carbon_kg"]
    ].sum()
    merged = base_periods.merge(grouped, on="period_start", how="left").fillna(0.0)
    x_data = [format_time(v, time_type) for v in merged["period_start"].tolist()]
    return (
        x_data,
        merged["unit_total_carbon_kg"].astype(float).tolist(),
        merged["electric_carbon_kg"].astype(float).tolist(),
        merged["chemical_carbon_kg"].astype(float).tolist(),
    )


def make_stage_router(
    *,
    data_dir_name: str,
    route_base: str,
    info_fields: list[dict],
    qtype_units: Dict[int, dict],
    share_label_key: str = "工艺单元",
) -> APIRouter:
    router = APIRouter()
    base_dir = os.path.dirname(os.path.dirname(__file__))
    data_dir = os.path.join(base_dir, "data", "real-time output", "process_stage_outputs", data_dir_name)

    def build_info(time_type: int) -> Dict[str, Any]:
        rows = detail_rows(data_dir, STATIC_INFO_TIME_TYPE)
        total = float(total_row(data_dir, STATIC_INFO_TIME_TYPE)["unit_total_carbon_kg_sum"])
        data = {
            "unit": CARBON_UNIT,
            "totalCarbonEmissions": total,
        }
        for item in info_fields:
            data[item["field"]] = sum_for_aliases(rows, item["aliases"], "unit_total_carbon_kg_sum")
        return round_obj({"code": 0, "msg": "", "data": data})

    def build_trend(body: TrendBody) -> Dict[str, Any]:
        time_config(body.timeType)
        q = qtype_units.get(body.qtype)
        if not q:
            allowed = "/".join(str(k) for k in sorted(qtype_units))
            raise HTTPException(status_code=400, detail=f"qtype 必须是 {allowed}")
        df = trend_table(data_dir, body.timeType)
        x_data, total_data, elec_data, chem_data = aggregate_trend(df, q["aliases"], body.timeType)
        name = q["label"]
        return round_obj({
            "code": 0,
            "msg": "",
            "data": {
                "id": "80",
                "styleType": "0",
                "customOption": {},
                "xAxis": [{"type": "category", "name": "", "data": x_data}],
                "yAxis": [{"name": "kgCO2e", "type": "value"}],
                "series": [
                    {"name": "总碳排", "type": "line", "data": total_data},
                    {"name": f"{name}电耗碳排", "type": "line", "data": elec_data},
                    {"name": f"{name}药耗碳排", "type": "line", "data": chem_data},
                ],
                "colors": COLORS,
            },
        })

    def build_share(time_type: int) -> Dict[str, Any]:
        rows = detail_rows(data_dir, time_type)
        source = []
        for q in qtype_units.values():
            value = sum_for_aliases(rows, q["aliases"], "unit_share_within_stage_avg") * 100.0
            source.append({share_label_key: q["share_label"], "数据值": value})
        return round_obj({
            "code": 0,
            "msg": "",
            "data": {
                "unit": "%",
                "dimensions": [share_label_key, "数据值"],
                "source": source,
                "dimensionsMapping": [share_label_key, "数据值"],
            },
        })

    @router.get(f"{route_base}/info")
    def info_get(timeType: int = Query(4)) -> Dict[str, Any]:
        return build_info(timeType)

    @router.post(f"{route_base}/info")
    def info_post(body: TimeBody) -> Dict[str, Any]:
        return build_info(int(body.timeType))

    @router.post(f"{route_base}/trend")
    def trend(body: TrendBody) -> Dict[str, Any]:
        return build_trend(body)

    @router.get(f"{route_base}/share")
    def share_get(timeType: int = Query(4)) -> Dict[str, Any]:
        return build_share(timeType)

    @router.post(f"{route_base}/share")
    def share_post(body: TimeBody = TimeBody()) -> Dict[str, Any]:
        return build_share(int(body.timeType))

    return router
