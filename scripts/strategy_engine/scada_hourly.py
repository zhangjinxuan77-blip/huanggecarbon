# -*- coding: utf-8 -*-
"""
L1 日间分时分析：从SCADA CSV提取指定日期的逐小时碳排数据。

数据来源：InfluxDB导出的1分钟粒度实测功率（activePower，单位kW）
计算方式：每条记录代表1分钟均值，kWh = kW × (1/60)，按小时聚合后乘电网排放因子
时区：CSV时间戳为UTC，转换为Asia/Shanghai（UTC+8）后按本地小时分桶
"""

from pathlib import Path
from datetime import date as date_type
import pandas as pd


POWER_CODES = [
    # 取水泵站瞬时功率（kW，积分法：kW×1min/60 = kWh）
    "LHS_QS_P1_EM_In_activePower",
    "LHS_QS_P2_EM_In_activePower",
    "LHS_QS_P3_EM_In_activePower",
    "LHS_QS_P4_EM_In_activePower",
    "LHS_QS_P5_EM_In_activePower",
    "LHS_QS_P6_EM_In_activePower",
    # 送水泵房瞬时功率
    "HGS_2_SSBF_EM60_In_activePower",
    # 注：原水提升泵组 SCADA 只有累计电能（activeEnergy），无瞬时功率，
    #     见下方 ENERGY_CODES → 小时差分法
]

# 原水提升泵组累计电能（kWh，差分法：每小时 max-min = 当小时增量）
ENERGY_CODES = [f"HGS_PG{i}_G_In_activeEnergy" for i in [1, 2, 3, 4, 6]]


def _analyze_generated_hourly(
    csv_path: str | Path,
    grid_ef: float,
    peak_threshold_pct: float,
) -> dict:
    """分析仓库公共生成结果中的滚动24小时碳排。"""
    frame = pd.read_csv(csv_path, encoding="utf-8-sig")
    carbon_column = next(
        (
            column
            for column in ("total_carbon_kg", "plant_total_carbon_kg")
            if column in frame.columns
        ),
        None,
    )
    if carbon_column is None:
        return {"available": False, "reason": "公共小时结果缺少碳排字段"}

    frame["_time_utc"] = pd.to_datetime(
        frame["period_start"], errors="coerce", format="ISO8601", utc=True
    )
    frame["_carbon"] = pd.to_numeric(frame[carbon_column], errors="coerce")
    frame = frame.dropna(subset=["_time_utc", "_carbon"]).sort_values("_time_utc").tail(24)
    if frame.empty:
        return {"available": False, "reason": "公共小时结果中没有有效数据"}

    frame["_time_local"] = frame["_time_utc"].dt.tz_convert("Asia/Shanghai")
    frame["hour"] = frame["_time_local"].dt.hour
    by_hour = frame.groupby("hour")["_carbon"].sum()
    hourly_carbon = [round(float(by_hour.get(hour, 0.0)), 2) for hour in range(24)]
    hourly_kwh = [round(value / grid_ef, 2) if grid_ef > 0 else 0.0 for value in hourly_carbon]

    nonzero = [(hour, value) for hour, value in enumerate(hourly_carbon) if value > 0]
    if not nonzero:
        return {"available": False, "reason": "公共小时结果的碳排均为零"}

    total_carbon = round(sum(hourly_carbon), 2)
    observed_hours = max(int(frame["hour"].nunique()), 1)
    daily_avg = total_carbon / observed_hours
    peak_hour, peak_carbon = max(nonzero, key=lambda item: item[1])
    valley_hour, valley_carbon = min(nonzero, key=lambda item: item[1])
    hours_above_avg = [
        hour
        for hour, value in nonzero
        if value > daily_avg * (1 + peak_threshold_pct)
    ]

    return {
        "available": True,
        "target_date": "rolling_24h",
        "window_start": frame["_time_local"].min().isoformat(),
        "window_end": frame["_time_local"].max().isoformat(),
        "codes_used": [f"public_generated.{carbon_column}"],
        "hourly_kwh": hourly_kwh,
        "hourly_carbon": hourly_carbon,
        "total_carbon": total_carbon,
        "daily_avg_carbon_per_hour": round(daily_avg, 2),
        "peak_hour": int(peak_hour),
        "valley_hour": int(valley_hour),
        "peak_carbon": round(float(peak_carbon), 2),
        "valley_carbon": round(float(valley_carbon), 2),
        "peak_valley_ratio": (
            round(float(peak_carbon / valley_carbon), 2) if valley_carbon > 0 else None
        ),
        "hours_above_avg": [int(hour) for hour in hours_above_avg],
        "peak_threshold_pct": peak_threshold_pct * 100,
        "pumps_by_hour": [0] * 24,
        "grid_ef": grid_ef,
        "source": "仓库公共滚动24小时生成结果",
    }


def analyze_hourly_carbon(
    csv_path: str | Path,
    target_date: date_type,
    grid_ef: float = 0.5271,
    peak_threshold_pct: float = 0.20,
) -> dict:
    """
    提取指定日期的分时碳排，返回L1诊断所需的结构化数据。

    Args:
        csv_path:           SCADA CSV文件路径
        target_date:        目标日期（CST本地日期）
        grid_ef:            电网排放因子 kgCO₂e/kWh（默认0.5271）
        peak_threshold_pct: 小时碳排超过日均多少比例时标记为高峰（默认20%）

    Returns:
        dict，键说明见下方注释
    """
    target_str = str(target_date)

    columns = pd.read_csv(csv_path, nrows=0, encoding="utf-8-sig").columns
    if "period_start" in columns and (
        "total_carbon_kg" in columns or "plant_total_carbon_kg" in columns
    ):
        return _analyze_generated_hourly(csv_path, grid_ef, peak_threshold_pct)

    # ── 检测文件格式（InfluxDB原始格式含3行表头；预处理CSV无表头行）────────────
    first = pd.read_csv(csv_path, nrows=1, header=None)
    is_influx = str(first.iloc[0, 0]).startswith("#")
    skiprows = 3 if is_influx else 0

    # ── 分块读取，保留当日 activePower 和 activeEnergy 行 ─────────────────────
    ALL_CODES = set(POWER_CODES) | set(ENERGY_CODES)
    chunks = pd.read_csv(
        csv_path, skiprows=skiprows, chunksize=500_000,
        usecols=["_time", "_value", "code"],
    )
    kept = []
    for chunk in chunks:
        if is_influx:
            mask = (
                chunk["code"].isin(ALL_CODES) &
                chunk["_time"].str.startswith(target_str[:7])
            )
        else:
            # 预处理CSV的_time已转为CST，格式 "2026-01-22 HH:MM:SS+08:00"
            mask = (
                chunk["code"].isin(ALL_CODES) &
                chunk["_time"].astype(str).str.startswith(target_str)
            )
        if mask.any():
            kept.append(chunk[mask])

    if not kept:
        return {"available": False, "reason": f"CSV中未找到 {target_date} 的功率/电能数据"}

    df = pd.concat(kept, ignore_index=True)

    # ── 时区转换 → 过滤到目标日期 ────────────────────────────────────────────
    if is_influx:
        df["_time"] = pd.to_datetime(df["_time"], utc=True, format="ISO8601")
        df["_time_cst"] = df["_time"].dt.tz_convert("Asia/Shanghai")
    else:
        df["_time_cst"] = pd.to_datetime(
            df["_time"], utc=False, format="ISO8601", errors="coerce"
        )
        df = df.dropna(subset=["_time_cst"])
    df["date_cst"] = df["_time_cst"].dt.date.astype(str)
    df = df[df["date_cst"] == target_str]

    if df.empty:
        return {"available": False, "reason": f"CSV中 {target_date} 当日无数据（时区转换后）"}

    # ── 逐小时聚合 ──────────────────────────────────────────────────────────────
    df["hour"] = df["_time_cst"].dt.hour
    df["_value"] = pd.to_numeric(df["_value"], errors="coerce")

    # 方法A：瞬时功率积分（POWER_CODES）：kW × (1/60 h) = kWh
    pwr_df = df[df["code"].isin(POWER_CODES)].copy()
    pwr_df["kwh"] = pwr_df["_value"].clip(lower=0) / 60.0
    pwr_hourly = pwr_df.groupby("hour")["kwh"].sum()

    # 方法B：累计电能差分（ENERGY_CODES）：每小时 max-min = 当小时增量 kWh
    eng_df = df[df["code"].isin(ENERGY_CODES)].copy()
    if not eng_df.empty:
        eng_hourly = (
            eng_df.groupby(["hour", "code"])["_value"]
            .agg(lambda x: max(x.max() - x.min(), 0.0))
            .groupby(level="hour").sum()
        )
    else:
        eng_hourly = pd.Series(dtype=float)

    hourly_kwh_series = pwr_hourly.add(eng_hourly, fill_value=0.0)
    hourly_kwh = [round(float(hourly_kwh_series.get(h, 0.0)), 2) for h in range(24)]
    hourly_carbon = [round(e * grid_ef, 2) for e in hourly_kwh]

    total_carbon = round(sum(hourly_carbon), 2)
    daily_avg = total_carbon / 24.0

    # ── 峰谷识别 ─────────────────────────────────────────────────────────────
    nonzero = [(h, c) for h, c in enumerate(hourly_carbon) if c > 0]
    if not nonzero:
        return {"available": False, "reason": "当日所有小时碳排为零"}

    peak_hour = max(nonzero, key=lambda x: x[1])[0]
    valley_hour = min(nonzero, key=lambda x: x[1])[0]
    peak_carbon = hourly_carbon[peak_hour]
    valley_carbon = hourly_carbon[valley_hour]
    peak_valley_ratio = round(peak_carbon / valley_carbon, 2) if valley_carbon > 0 else None

    hours_above_avg = [
        h for h, c in enumerate(hourly_carbon)
        if c > 0 and c > daily_avg * (1 + peak_threshold_pct)
    ]

    # ── 每小时活跃泵台数（仅基于瞬时功率码，辅助信息）──────────────────────────
    if not pwr_df.empty:
        active_pumps_per_hour = (
            pwr_df[pwr_df["_value"] > 10]       # >10 kW 视为运行
            .groupby(["hour", "code"])["kwh"]
            .count()
            .reset_index()
            .groupby("hour")["code"]
            .nunique()
        )
    else:
        active_pumps_per_hour = pd.Series(dtype=int)
    pumps_by_hour = [int(active_pumps_per_hour.get(h, 0)) for h in range(24)]

    return {
        "available": True,
        "target_date": target_str,
        "codes_used": sorted(df["code"].unique().tolist()),
        "hourly_kwh": hourly_kwh,
        "hourly_carbon": hourly_carbon,
        "total_carbon": total_carbon,
        "daily_avg_carbon_per_hour": round(daily_avg, 2),
        "peak_hour": peak_hour,
        "valley_hour": valley_hour,
        "peak_carbon": peak_carbon,
        "valley_carbon": valley_carbon,
        "peak_valley_ratio": peak_valley_ratio,
        "hours_above_avg": hours_above_avg,
        "peak_threshold_pct": peak_threshold_pct * 100,
        "pumps_by_hour": pumps_by_hour,
        "grid_ef": grid_ef,
    }
