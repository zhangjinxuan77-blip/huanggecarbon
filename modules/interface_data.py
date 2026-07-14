# -*- coding: utf-8 -*-
"""Helpers shared by the static dashboard interface modules."""

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel, ValidationError


class ApiEnvelope(BaseModel):
    code: int
    msg: str
    data: dict[str, Any]


def _shared_periods(data_path: Path) -> tuple[str, str]:
    """读取公共结果的最新日期和最新完整日报日期。"""
    daily_path = (
        data_path.parent
        / "real-time output"
        / "scope123_总汇总"
        / "latest_7d_daily.csv"
    )
    hourly_path = (
        data_path.parent
        / "real-time output"
        / "scope123_总汇总"
        / "latest_24h_hourly.csv"
    )
    if not daily_path.is_file() or not hourly_path.is_file():
        raise HTTPException(status_code=500, detail="未找到公共日报数据文件")

    try:
        with daily_path.open(encoding="utf-8-sig", newline="") as file:
            dates = []
            for row in csv.DictReader(file):
                value = str(row.get("period_start", "")).strip()
                if value:
                    dates.append(datetime.fromisoformat(value.replace("Z", "+00:00")).date())
        with hourly_path.open(encoding="utf-8-sig", newline="") as file:
            hours = []
            for row in csv.DictReader(file):
                value = str(row.get("period_start", "")).strip()
                if value:
                    hours.append(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except (OSError, ValueError) as exc:
        raise HTTPException(status_code=500, detail="公共日报日期读取失败") from exc

    if not dates or not hours:
        raise HTTPException(status_code=500, detail="公共日报不含有效日期")

    unique_hours = sorted(set(hours))
    latest_window = unique_hours[-24:]
    if len(latest_window) < 24 or any(
        current - previous != timedelta(hours=1)
        for previous, current in zip(latest_window, latest_window[1:])
    ):
        raise HTTPException(status_code=500, detail="公共滚动小时数据不足24条或时间不连续")

    latest_date = max(dates)
    latest_hour = latest_window[-1]
    if latest_date != latest_hour.date():
        raise HTTPException(
            status_code=500,
            detail="公共日报与滚动小时数据的最新日期不一致",
        )
    complete_ceiling = latest_hour.date()
    if latest_hour.hour < 23:
        complete_ceiling -= timedelta(days=1)
    complete_dates = [value for value in dates if value <= complete_ceiling]
    if not complete_dates:
        raise HTTPException(status_code=500, detail="公共日报不含完整日期")
    return latest_date.isoformat(), max(complete_dates).isoformat()


def _validate_interface_period(data_path: Path, data: dict[str, Any]) -> None:
    """实时接口跟随最新行，策略接口跟随最新完整日报。"""
    meta = data.get("_meta")
    if not isinstance(meta, dict):
        raise HTTPException(status_code=409, detail="策略数据缺少周期元信息")

    strategy_date = meta.get("strategyDate", meta.get("date"))
    realtime_date = meta.get("realtimeDate")
    shared_latest, shared_complete = _shared_periods(data_path)
    if realtime_date != shared_latest:
        raise HTTPException(
            status_code=409,
            detail=(
                f"实时数据日期{realtime_date or '缺失'}与公共最新日期"
                f"{shared_latest}不一致"
            ),
        )
    if strategy_date != shared_complete:
        raise HTTPException(
            status_code=409,
            detail=(
                f"策略数据日期{strategy_date or '缺失'}与公共最新完整日期"
                f"{shared_complete}不一致"
            ),
        )


def load_interface_data(path: str) -> dict[str, Any]:
    data_path = Path(path)
    if not data_path.is_file():
        raise HTTPException(status_code=404, detail="未找到接口数据文件")

    try:
        with data_path.open(encoding="utf-8") as file:
            data = json.load(file)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise HTTPException(status_code=500, detail="接口数据文件格式错误") from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail="读取接口数据文件失败") from exc

    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="接口数据文件顶层必须是 JSON 对象")
    return data


def get_interface_response(path: str, key: str) -> ApiEnvelope:
    data_path = Path(path)
    data = load_interface_data(path)
    _validate_interface_period(data_path, data)
    response = data.get(key)
    if response is None:
        raise HTTPException(status_code=404, detail=f"数据中不含 {key}")
    if not isinstance(response, dict):
        raise HTTPException(status_code=500, detail=f"接口数据 {key} 格式错误")
    try:
        return ApiEnvelope.model_validate(response)
    except ValidationError as exc:
        raise HTTPException(status_code=500, detail=f"接口数据 {key} 响应结构错误") from exc
