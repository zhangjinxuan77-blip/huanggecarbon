# -*- coding: utf-8 -*-
"""
基于仓库公共生成结果生成诊断日报
用法：python generate_scada_report.py [YYYY-MM-DD]
      不传日期时使用仓库公共结果中的最新完整日报日期
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import argparse
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

import json
import pandas as pd
from datetime import datetime, timedelta

from models import (
    PlantData, CoagulationData, FiltrationData, DisinfectionData,
    PumpStationData, SludgeDewatering, ProcessUnits, ReportRequest
)
from rules_engine import evaluate
from renderer import generate_report

GRID_EF = 0.5271


def _find_repository_root() -> Path:
    """兼容 Development 工作目录和 GitHub 仓库内 scripts/strategy_engine。"""
    script_dir = Path(__file__).resolve().parent
    for ancestor in (script_dir, *script_dir.parents):
        for candidate in (ancestor, ancestor / "huanggecarbon"):
            if (
                (candidate / "data" / "real-time output").is_dir()
                and (candidate / "modules").is_dir()
            ):
                return candidate
    raise FileNotFoundError("无法定位 huanggecarbon 仓库及公共生成数据目录")


SCRIPT_DIR = Path(__file__).resolve().parent
REPOSITORY_ROOT = _find_repository_root()
RUNNING_IN_REPOSITORY = (
    REPOSITORY_ROOT == SCRIPT_DIR or REPOSITORY_ROOT in SCRIPT_DIR.parents
)
BASE = SCRIPT_DIR / "output" if RUNNING_IN_REPOSITORY else SCRIPT_DIR.parent
BASE.mkdir(parents=True, exist_ok=True)
API_DATA_DIR = Path(os.getenv(
    "CARBON_API_DATA_DIR",
    REPOSITORY_ROOT / "data",
))
SHARED_OUTPUT_DIR = API_DATA_DIR / "real-time output"
SHARED_DAILY_PATH = SHARED_OUTPUT_DIR / "scope123_总汇总" / "latest_7d_daily.csv"
SHARED_HOURLY_PATH = SHARED_OUTPUT_DIR / "scope123_总汇总" / "latest_24h_hourly.csv"


def _read_output(relative_path: str) -> pd.DataFrame:
    path = SHARED_OUTPUT_DIR / relative_path
    if not path.is_file():
        raise FileNotFoundError(f"未找到公共生成结果：{path}")
    return pd.read_csv(path, encoding="utf-8-sig")


def _timestamps(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        raise KeyError(f"公共生成结果缺少时间字段：{column}")
    values = pd.to_datetime(frame[column], errors="coerce", format="ISO8601", utc=True)
    if values.dropna().empty:
        raise ValueError(f"公共生成结果的时间字段无有效值：{column}")
    return values


def _row_for_date(frame: pd.DataFrame, column: str, target_date) -> pd.Series:
    values = _timestamps(frame, column)
    selected = frame.loc[values.dt.date == target_date]
    if selected.empty:
        raise ValueError(f"公共生成结果中缺少 {target_date}（字段 {column}）")
    return selected.iloc[-1]


def _number(row: pd.Series, column: str, default: float = 0.0) -> float:
    value = pd.to_numeric(pd.Series([row.get(column)]), errors="coerce").iloc[0]
    return default if pd.isna(value) else float(value)


def _shared_periods(scope_daily: pd.DataFrame, scope_hourly: pd.DataFrame):
    """返回公共结果的最新日期和最新完整日报日期（UTC日桶）。"""
    daily_dates = sorted(set(_timestamps(scope_daily, "period_start").dt.date))
    latest_hour = _timestamps(scope_hourly, "period_start").max()
    latest_date = daily_dates[-1]

    # 当前UTC日尚未走到23时，则最新日汇总仍是部分日，只能用于实时展示。
    complete_ceiling = latest_hour.date()
    if latest_hour.hour < 23:
        complete_ceiling -= timedelta(days=1)
    complete_dates = [value for value in daily_dates if value <= complete_ceiling]
    if not complete_dates:
        raise ValueError("公共生成结果中没有可用的完整日报日期")
    return latest_date, complete_dates[-1], latest_hour


scope_daily = _read_output("scope123_总汇总/latest_7d_daily.csv")
scope_hourly = _read_output("scope123_总汇总/latest_24h_hourly.csv")
scope2_daily = _read_output("scope2_电耗汇总/latest_7d_daily.csv")
scope3_daily = _read_output("scope3_汇总/latest_7d_daily.csv")
scope1_daily = _read_output(
    "scope1_carbon_outputs/21_范围一_臭氧泄漏率和臭氧实际产量/latest_7d_daily.csv"
)
water_daily = _read_output("单位水处理强度和减排量/单位水处理强度_日趋势.csv")

REALTIME_DATE, COMPLETE_REPORT_DATE, LATEST_SHARED_HOUR = _shared_periods(
    scope_daily, scope_hourly
)

parser = argparse.ArgumentParser()
parser.add_argument("date", nargs="?", help="报告日期 YYYY-MM-DD；必须是公共结果中的完整日报")
args   = parser.parse_args()

if args.date:
    requested_date = datetime.strptime(args.date, "%Y-%m-%d")
    available_dates = set(_timestamps(scope_daily, "period_start").dt.date)
    if requested_date.date() not in available_dates:
        parser.error(
            f"公共日报中不存在指定日期 {requested_date:%Y-%m-%d}"
        )
    if requested_date.date() > COMPLETE_REPORT_DATE:
        parser.error(
            f"指定日期 {requested_date:%Y-%m-%d} 尚未形成完整日报；"
            f"最新完整日期为 {COMPLETE_REPORT_DATE}"
        )
    REPORT_DATE = requested_date
else:
    REPORT_DATE = datetime.combine(COMPLETE_REPORT_DATE, datetime.min.time())
    args.date = REPORT_DATE.strftime("%Y-%m-%d")

def _chemical_result(prefixes, target_date) -> dict[str, float]:
    root = SHARED_OUTPUT_DIR / "chemical_carbon_outputs"
    chemical = 0.0
    carbon = 0.0
    matched = 0
    for prefix in prefixes:
        for path in root.glob(f"{prefix}_*/latest_7d_daily.csv"):
            frame = pd.read_csv(path, encoding="utf-8-sig")
            time_column = next(
                (name for name in ("hour", "period_start", "time") if name in frame.columns),
                None,
            )
            if time_column is None:
                continue
            row = _row_for_date(frame, time_column, target_date)
            chemical += _number(row, "unit_total_chemical_kg")
            carbon += _number(row, "unit_total_carbon_kg")
            matched += 1
    if matched == 0:
        raise ValueError(f"公共药耗结果中缺少单元：{prefixes}")
    return {"chemical_kg": chemical, "carbon_kg": carbon}


def _monthly_chemical_series(prefixes) -> dict[tuple[int, int], float]:
    root = SHARED_OUTPUT_DIR / "chemical_carbon_outputs"
    result: dict[tuple[int, int], float] = {}
    for prefix in prefixes:
        for path in root.glob(f"{prefix}_*/latest_12m_monthly.csv"):
            frame = pd.read_csv(path, encoding="utf-8-sig")
            time_column = next(
                (name for name in ("hour", "period_start", "time") if name in frame.columns),
                None,
            )
            if time_column is None:
                continue
            for index, timestamp in _timestamps(frame, time_column).items():
                if pd.isna(timestamp):
                    continue
                key = (timestamp.year, timestamp.month)
                result[key] = result.get(key, 0.0) + _number(
                    frame.loc[index], "unit_total_chemical_kg"
                )
    return result


def _shared_monthly_baselines(report_date) -> dict[str, object]:
    """用公共月度生成结果计算报告日前3个完整月的策略基准。"""
    scope2_monthly = _read_output("scope2_电耗汇总/latest_12m_monthly.csv")
    water_monthly = _read_output("单位水处理强度和减排量/单位水处理强度_月趋势.csv")

    water_by_month = {}
    for index, timestamp in _timestamps(water_monthly, "scope月份").items():
        if not pd.isna(timestamp):
            water_by_month[(timestamp.year, timestamp.month)] = _number(
                water_monthly.loc[index], "水处理量_m3"
            )

    energy_by_month = {}
    month_days = {}
    for index, timestamp in _timestamps(scope2_monthly, "period_start").items():
        if not pd.isna(timestamp):
            key = (timestamp.year, timestamp.month)
            energy_by_month[key] = _number(
                scope2_monthly.loc[index], "plant_total_carbon_kg"
            )
            month_days[key] = timestamp.day

    pac_by_month = _monthly_chemical_series(["15"])
    naclo_by_month = _monthly_chemical_series(["17", "18"])
    report_key = (report_date.year, report_date.month)
    common_months = sorted(
        key
        for key in set(water_by_month) & set(energy_by_month)
        & set(pac_by_month) & set(naclo_by_month)
        if key < report_key and water_by_month[key] > 0 and month_days[key] > 0
    )[-3:]
    if not common_months:
        raise ValueError("公共月度生成结果不足，无法计算策略基准")

    return {
        "energy_carbon": sum(
            energy_by_month[key] / month_days[key] for key in common_months
        ) / len(common_months),
        "pac_unit": sum(
            pac_by_month[key] / (water_by_month[key] / 1000.0)
            for key in common_months
        ) / len(common_months),
        "naclo_unit": sum(
            naclo_by_month[key] / (water_by_month[key] / 1000.0)
            for key in common_months
        ) / len(common_months),
        "months": [f"{year:04d}-{month:02d}" for year, month in common_months],
    }


# ── 1. 公共完整日报数据 ───────────────────────────────────────────────────────
report_day = REPORT_DATE.date()
shared_baselines = _shared_monthly_baselines(report_day)
scope_row = _row_for_date(scope_daily, "period_start", report_day)
scope2_row = _row_for_date(scope2_daily, "period_start", report_day)
scope3_row = _row_for_date(scope3_daily, "period_start", report_day)
scope1_row = _row_for_date(scope1_daily, "hour", report_day)
water_row = _row_for_date(water_daily, "scope日期", report_day)

total_energy_kwh = _number(scope2_row, "plant_total_electricity_kwh")
carbon_energy = _number(scope2_row, "plant_total_carbon_kg")
pg_total = _number(scope2_row, "intake_electricity_kwh")
ssbf_total = _number(scope2_row, "supply_electricity_kwh")
inside_total = _number(scope2_row, "inside_plant_electricity_kwh")
water_volume_m3 = _number(water_row, "水处理量_m3")
volume_source = "公共生成结果（2024模拟水量按同月同日匹配）"

# 臭氧日汇总中的 A_leak_rate 被按小时求和，不能直接作为比例；由总泄漏量/实际产量还原。
O3_KG = _number(scope1_row, "A_actual_ozone_kg")
O3_LEAKAGE_KG = _number(scope1_row, "unit_total_chemical_kg")
O3_A_LEAKAGE_KG = _number(scope1_row, "A_ozone_leak_kg")
O3_LEAKAGE_RATE = O3_A_LEAKAGE_KG / O3_KG if O3_KG > 0 else 0.0
CARBON_O3 = _number(scope_row, "scope1_carbon_kg")

pac_result = _chemical_result(["15"], report_day)
pam_result = _chemical_result(["16"], report_day)
naclo_result = _chemical_result(["17", "18"], report_day)
PAC_KG = pac_result["chemical_kg"]
PAM_KG = pam_result["chemical_kg"]
NACLO_KG = naclo_result["chemical_kg"]
chemical_carbon = _number(scope3_row, "chemical_carbon_kg")
chemical_source = "仓库公共药耗生成结果"

sludge_carbon = _number(scope3_row, "sludge_transport_carbon_kg")
_sludge = {
    "sludge_weight_tons": _number(scope3_row, "sludge_transport_t"),
    "sludge_solid_rate": _number(scope3_row, "sludge_solid_rate"),
    "sludge_transport_km": _number(scope3_row, "sludge_transport_distance_km"),
}

print(f"公共完整日报（{args.date}）:")
print(f"  全厂用电:     {total_energy_kwh:,.1f} kWh")
print(f"  取水段:       {pg_total:,.1f} kWh")
print(f"  送水段:       {ssbf_total:,.1f} kWh")
print(f"  厂内其他:     {inside_total:,.1f} kWh")
print(f"  供水量:       {water_volume_m3:,.0f} m³（{volume_source}）")
print(f"  PAC/PAM/NaClO: {PAC_KG:.1f}/{PAM_KG:.1f}/{NACLO_KG:.1f} kg")
print(f"  污泥运输:     {_sludge['sludge_weight_tons']:.2f} t / {sludge_carbon:.2f} kgCO2e")

# ── 5. 构造报告 ────────────────────────────────────────────────────────────────
plant = PlantData(
    plant_id="南沙黄阁水厂",
    timestamp=REPORT_DATE,
    period="日报",
    total_energy_consumption=round(total_energy_kwh, 2),
    carbon_emission_energy=carbon_energy,
    carbon_emission_chemical=chemical_carbon,
    carbon_emission_o3_leakage=CARBON_O3,
    carbon_emission_sludge_transport=sludge_carbon,
    water_volume_m3=water_volume_m3,
)

coagulation   = CoagulationData(pac_consumption_kg=PAC_KG, pam_consumption_kg=PAM_KG)
filtration    = FiltrationData()
disinfection  = DisinfectionData(
    sodium_hypochlorite_consumption_kg=NACLO_KG,
    ozone_consumption_kg=O3_KG,
    ozone_leakage_rate=O3_LEAKAGE_RATE,
)

pump_stations = []
if pg_total > 0:
    pump_stations.append(PumpStationData(station_id="取水泵站", energy_consumption=round(pg_total, 2)))
if ssbf_total > 0:
    pump_stations.append(PumpStationData(station_id="送水泵房", energy_consumption=round(ssbf_total, 2)))
if inside_total > 0:
    pump_stations.append(PumpStationData(station_id="厂内其他用电", energy_consumption=round(inside_total, 2)))

sludge_dewatering = SludgeDewatering(
    pam_consumption_kg=PAM_KG,
    **(_sludge if _sludge else {}),
)

units = ProcessUnits(
    coagulation_sedimentation=coagulation,
    filtration=filtration,
    disinfection=disinfection,
    pump_stations=pump_stations,
    pipeline_segments=[],
    sludge_dewatering=sludge_dewatering,
)

previous_plant = None
previous_dates = sorted(
    value for value in set(_timestamps(scope_daily, "period_start").dt.date)
    if value < report_day
)
if previous_dates:
    previous_day = previous_dates[-1]
    previous_scope = _row_for_date(scope_daily, "period_start", previous_day)
    previous_scope2 = _row_for_date(scope2_daily, "period_start", previous_day)
    previous_scope3 = _row_for_date(scope3_daily, "period_start", previous_day)
    previous_water = _row_for_date(water_daily, "scope日期", previous_day)
    previous_plant = PlantData(
        plant_id="南沙黄阁水厂",
        timestamp=datetime.combine(previous_day, datetime.min.time()),
        period="日报",
        total_energy_consumption=_number(previous_scope2, "plant_total_electricity_kwh"),
        carbon_emission_energy=_number(previous_scope2, "plant_total_carbon_kg"),
        carbon_emission_chemical=_number(previous_scope3, "chemical_carbon_kg"),
        carbon_emission_o3_leakage=_number(previous_scope, "scope1_carbon_kg"),
        carbon_emission_sludge_transport=_number(
            previous_scope3, "sludge_transport_carbon_kg"
        ),
        water_volume_m3=_number(previous_water, "水处理量_m3"),
    )

request = ReportRequest(plant=plant, units=units, previous_plant=previous_plant)
# 分时分析直接读取其他工程师生成的滚动24小时碳排结果。
flags   = evaluate(
    request,
    scada_csv=str(SHARED_HOURLY_PATH),
    baselines=shared_baselines,
)
report  = generate_report(request, flags)

# ── 6. 保存报告 TXT ──────────────────────────────────────────────────────────
out_txt = BASE / f"南沙黄阁水厂_运行分析报告_{REPORT_DATE.strftime('%Y%m%d')}.txt"
out_txt.write_text(report.full_report_text, encoding="utf-8")
print(f"\n报告已保存：{out_txt.name}")

# ── 7. 保存碳排数据 TXT ───────────────────────────────────────────────────────
carbon_ef   = round(plant.carbon_emission_energy, 0)
c_pump      = round(carbon_energy, 0)
c_intake    = round(_number(scope2_row, "intake_carbon_kg"), 0)
c_delivery  = round(_number(scope2_row, "supply_carbon_kg"), 0)
c_pac       = round(pac_result["carbon_kg"], 2)
c_pam       = round(pam_result["carbon_kg"], 2)
c_naclo     = round(naclo_result["carbon_kg"], 2)
c_sludge    = round(sludge_carbon, 2)

carbon_txt = f"""碳排放数据汇总
日期：{REPORT_DATE.strftime('%Y-%m-%d')}    水厂：南沙黄阁水厂
════════════════════════════════════════════════════

范围1

  O₃泄露碳排放量          {CARBON_O3:.2f} kgCO₂e
  O₃投加量               {O3_KG:.2f} kg
  O₃泄露量               {O3_LEAKAGE_KG:.2f} kg

════════════════════════════════════════════════════

范围2

  范围2总碳排放量          {int(carbon_ef):,} kgCO₂e
  全厂电耗碳排量           {int(c_pump):,} kgCO₂e
  全厂总用电量             {int(total_energy_kwh):,} kWh
  取水段用电量             {int(pg_total):,} kWh
  取水段电耗碳排           {int(c_intake):,} kgCO₂e
  送水段用电量             {int(ssbf_total):,} kWh
  送水段电耗碳排           {int(c_delivery):,} kgCO₂e

════════════════════════════════════════════════════

范围3

  范围3总碳排量            {int(plant.carbon_emission_chemical + (c_sludge if _sludge else 0)):,} kgCO₂e
  PAC碳排               {int(c_pac):,} kgCO₂e
  PAM碳排               {int(c_pam):,} kgCO₂e
  NaClO碳排             {int(c_naclo):,} kgCO₂e
  污泥运输碳排放量          {int(c_sludge):,} kgCO₂e
    污泥量               {_sludge['sludge_weight_tons'] if _sludge else 'N/A'} 吨
    含固率               {f"{_sludge['sludge_solid_rate']*100:.0f}%" if _sludge else 'N/A'}
    运输距离             {_sludge['sludge_transport_km'] if _sludge else 'N/A'} km

════════════════════════════════════════════════════
"""

out_carbon = BASE / f"南沙黄阁水厂_碳排数据_{REPORT_DATE.strftime('%Y%m%d')}.txt"
out_carbon.write_text(carbon_txt, encoding="utf-8")
print(f"碳排数据已保存：{out_carbon.name}")

# ── 8. 保存优化策略 TXT ───────────────────────────────────────────────────────
raw     = flags.get("raw_data_summary", {})
l1      = flags.get("l1_flags", [])
l3d     = flags.get("layer3_details", {})
l3t     = set(flags.get("layer3_rules_triggered", []))
l1d     = flags.get("layer1_details", {})

# 范围1
s1_lines = ["范围1  直接排放（O₃泄露）", ""]
if raw.get("ozone_leakage_high"):
    s1_lines.append("  ⚠ O₃泄露率偏高，建议：")
    s1_lines.append("  → 检查臭氧发生器密封件及管道接口，排查泄露点。")
    s1_lines.append("  → 建议安装在线泄露监测仪，实现实时预警。")
elif O3_LEAKAGE_RATE >= 0.05 * 0.9:
    s1_lines.append("  ⚠ O₃泄露率接近阈值，建议加强在线监测并维持当前密封维护频率。")
else:
    s1_lines.append("  ✓ O₃泄露率正常，建议维持当前密封维护频率。")
    s1_lines.append("  → 建议定期校准臭氧浓度检测仪，确保计量准确。")

# 范围2
s2_lines = ["范围2  间接排放（电力）", ""]
if raw.get("energy_baseline_triggered"):
    s2_lines.append("  ⚠ 电耗碳排超出月度基准，建议：")
    s2_lines.append("  → 建议结合能耗与药耗变化，优先排查送水泵房与臭氧系统运行效率。")
else:
    s2_lines.append("  ✓ 电耗碳排处于正常范围。")
l1_hourly = l3d.get("L1", {})
if isinstance(l1_hourly, dict) and l1_hourly.get("hours_above_avg"):
    s2_lines.append("  → 建议核查高碳时段的运行设备和调度记录，确认可移峰负荷后再调整排程。")
else:
    s2_lines.append("  → 未识别显著高碳时段，建议维持当前排程并持续监测分时电耗。")

# 范围3
s3_lines = ["范围3  其他间接排放（药剂 + 污泥运输）", ""]
l2_pac = l3d.get("L2", {})
if "L2" in l3t and isinstance(l2_pac, dict) and l2_pac.get("deviation_pct", 0) > 0:
    s3_lines.append("  ⚠ PAC单耗偏高，建议：")
    s3_lines.append("  → 建议检查混凝剂投加泵校准状态，复核混合搅拌器运行效率。")
    s3_lines.append("  → 结合原水浊度变化，优化PAC投加曲线。")
else:
    s3_lines.append("  ✓ PAC投加量正常。")
    s3_lines.append("  → 建议持续跟踪原水浊度，动态调整投加策略。")
l3_naclo = l3d.get("L3", {})
if "L3" in l3t:
    s3_lines.append("  ⚠ NaClO单耗偏差较大，建议：")
    s3_lines.append("  → 建议根据水温季节变化动态调整工艺参数预设值。")
else:
    s3_lines.append("  ✓ NaClO投加量正常。")
    s3_lines.append("  → NaClO单耗未触发异常，建议维持当前参数并持续监测。")
if "carbon_structure_shift" in l1:
    s3_lines.append("  → 建议关注药剂投加系统运行状态，复核投加量或药剂类型。")
s3_lines.append("  → 当前仅有单期污泥数据，建议形成连续基线后再优化PAM与运输参数。")

div = "════════════════════════════════════════════════════"
strategy_txt = "\n".join([
    "优化策略汇总",
    f"日期：{REPORT_DATE.strftime('%Y-%m-%d')}    水厂：南沙黄阁水厂",
    div, "",
    "\n".join(s1_lines), "",
    div, "",
    "\n".join(s2_lines), "",
    div, "",
    "\n".join(s3_lines), "",
    div,
])

out_strategy = BASE / f"南沙黄阁水厂_优化策略_{REPORT_DATE.strftime('%Y%m%d')}.txt"
out_strategy.write_text(strategy_txt, encoding="utf-8")
print(f"优化策略已保存：{out_strategy.name}")

# ── 9. 表盘格式优化策略面板 ────────────────────────────────────────────────────
O3_THRESHOLD   = 0.05
o3_leak_rate   = O3_LEAKAGE_RATE
o3_over        = max(0.0, o3_leak_rate - O3_THRESHOLD)
o3_over_str    = f"{o3_over*100:.1f}%" if o3_over > 0 else "0（未超标）"
if raw.get("ozone_leakage_high"):
    o3_strategy = "O3超阈值，检查密封件及管道接口"
elif o3_leak_rate >= O3_THRESHOLD * 0.9:
    o3_strategy = "O3接近阈值，建议加强监测和密封维护"
else:
    o3_strategy = "O3未超阈值，维持密封维护和监测"

# 范围2：能耗热点
pump_names     = [ps["station_id"] for ps in flags.get("raw_data_summary", {}).get("pump_stations", [])]
heat_pump      = ("、".join(pump_names) if pump_names else "原水提升泵房") \
                 if raw.get("energy_baseline_triggered") else "无"
e_vs_base      = raw.get("energy_vs_baseline_pct")
e_base_str     = f"电耗碳排较月度历史均值偏高 +{e_vs_base:.1f}%" if e_vs_base and e_vs_base > 0 \
                 else "电耗碳排处于正常范围"
if raw.get("energy_baseline_triggered"):
    e2_strategy = "建议结合能耗与药耗变化，优先排查送水泵房与臭氧系统运行效率"
else:
    e2_strategy = "电耗未触发异常，建议维持当前调度并持续监测分时电耗"

# 范围3：PAC
l2_pac         = l3d.get("L2", {})
pac_today      = l2_pac.get("pac_unit_today", PAC_KG / (water_volume_m3 / 1000) if water_volume_m3 else 0)
pac_base       = l2_pac.get("pac_baseline_monthly")
pac_dev        = l2_pac.get("deviation_pct")
pac_comparable = pac_base is not None and pac_dev is not None
if "L2" in l3t:
    pac_heat = "PAC投加"
    pac_cause = "PAC单耗偏高，可能与水质或投加设备效率有关"
    pac_strategy = "建议检查混凝剂投加泵校准状态，复核混合搅拌器运行效率。结合原水浊度变化，优化PAC投加曲线"
else:
    pac_heat = "各药剂投加正常"
    pac_cause = "PAC单耗未触发异常，建议持续跟踪投加量与原水浊度"
    pac_strategy = "建议持续跟踪原水浊度，动态调整投加策略"

panel_txt = f"""优化策略（表盘格式）
日期：{REPORT_DATE.strftime('%Y-%m-%d')}    水厂：南沙黄阁水厂
{div}

范围1

  O3泄漏率标准：阈值 {O3_THRESHOLD*100:.0f}%
  O3实际泄漏率：{o3_leak_rate*100:.1f}%    超标量：{o3_over_str}
  O3投加量：{O3_KG:.2f} kg    泄漏量：{O3_KG * o3_leak_rate:.2f} kg

  优化策略：{o3_strategy}

{div}

范围2

  能耗热点：{heat_pump}
  可能原因：{e_base_str}

  优化策略：{e2_strategy}

{div}

范围3

  高碳排热点：{pac_heat}

  PAC投加：{PAC_KG:.1f} kg    单耗：{pac_today:.3f} kg/千吨水    前3月基准：{f"{pac_base:.3f} kg/千吨水" if pac_base else "N/A"}    偏差：{f"{pac_dev:+.1f}%（超出阈值±20%）" if pac_dev else "N/A"}

  可能原因：{pac_cause}

  优化策略：{pac_strategy}

{div}
"""

out_panel = BASE / f"南沙黄阁水厂_优化策略面板_{REPORT_DATE.strftime('%Y%m%d')}.txt"
out_panel.write_text(panel_txt, encoding="utf-8")
print(f"优化策略面板已保存：{out_panel.name}")

# ── 10. 前端对接 JSON ─────────────────────────────────────────────────────────
# 按当前前端接口路径组织，覆盖所有可从当日数据生成的端点
month_name  = f"{REPORT_DATE.month}月"
l1_hourly   = l3d.get("L1", {})
l2_pac_d    = l3d.get("L2", {})
l3_naclo_d  = l3d.get("L3", {})
scope3_total = _number(scope3_row, "scope3_total_carbon_kg")

# ── /api/dashboard/diagnosis_page?type=1  优化策略-范围1 ──────────────────────
scope1_strategy = o3_strategy
api_diagnosis_s1 = {
    "code": 0, "msg": "",
    "data": {
        "standardLeakageRate":  f"阈值{O3_THRESHOLD*100:.0f}%",
        "actualLeakageRate":    f"{O3_LEAKAGE_RATE*100:.1f}%",
        "dosage":               str(round(O3_KG, 2)),
        "leakageVolume":        str(round(O3_LEAKAGE_KG, 2)),
        "optimizationStrategy": scope1_strategy,
    }
}

# ── /api/dashboard/diagnosis_page?type=2  优化策略-范围2 ──────────────────────
e_vs_base    = raw.get("energy_vs_baseline_pct")
e2_cause     = (f"电耗碳排较月度历史均值偏高+{e_vs_base:.1f}%"
                if e_vs_base and e_vs_base > 0 else "电耗碳排处于正常范围，未发现明显异常")
# 无能耗异常时热点显示"无"，有异常才显示具体站房
heat_pump_s2 = heat_pump
api_diagnosis_s2 = {
    "code": 0, "msg": "",
    "data": {
        "energyConsumptionHotspots": heat_pump_s2,
        "possibleCauses":            e2_cause,
        "optimizationStrategy":      e2_strategy,
    }
}

# ── /api/dashboard/diagnosis_page?type=3  优化策略-范围3 ──────────────────────
pac_analysis = (f"PAC投加{PAC_KG:.0f}kg，单耗{pac_today:.2f}kg/千吨水，偏差{pac_dev:+.1f}%"
                if pac_comparable else f"PAC投加{PAC_KG:.0f}kg，单耗{pac_today:.2f}kg/千吨水")
api_diagnosis_s3 = {
    "code": 0, "msg": "",
    "data": {
        "energyConsumptionHotspots": pac_heat,
        "dataAnalysis":              pac_analysis,
        "possibleCauses":            pac_cause,
        "optimizationStrategy":      pac_strategy,
    }
}


def _validate_diagnosis_contract(payload: dict, limits: dict[str, int], name: str) -> None:
    """校验前端诊断接口规定的字符串类型与最大字符数。"""
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError(f"{name}.data 必须是对象")
    for field, max_chars in limits.items():
        value = data.get(field)
        if not isinstance(value, str):
            raise ValueError(f"{name}.{field} 必须是字符串")
        if len(value) > max_chars:
            raise ValueError(
                f"{name}.{field} 超过前端限制：{len(value)}/{max_chars}字符"
            )


_validate_diagnosis_contract(
    api_diagnosis_s1,
    {"optimizationStrategy": 20},
    "diagnosis_page?type=1",
)
_validate_diagnosis_contract(
    api_diagnosis_s2,
    {"possibleCauses": 50, "optimizationStrategy": 30},
    "diagnosis_page?type=2",
)
_validate_diagnosis_contract(
    api_diagnosis_s3,
    {
        "dataAnalysis": 50,
        "possibleCauses": 25,
        "optimizationStrategy": 50,
    },
    "diagnosis_page?type=3",
)

# ── /api/dashboard/lowcarbon/strategies  工艺策略-单元优化 ────────────────────
# 保留原有指标字段，并为每个工艺单元补充可直接展示的优化策略。
# 数值缺失时按0继续输出策略，同时由数据来源字段保留可追溯性。
disinfection_strategy = o3_strategy
pumping_strategy = e2_strategy
dewatering_strategy = (
    "建议优化脱水设备运行时段与污泥运输批次，降低PAM消耗和运输碳排"
    if _sludge else "脱水间未触发异常，建议维持当前运行并持续监测"
)
api_strategies = {
    "code": 0, "msg": "",
    "data": {
        "coagulationSedimentation": {
            "pacDosagePerTon": str(round(pac_today, 3)),
            "optimizationStrategy": pac_strategy,
        },
        "disinfection": {
            "leakageRate": f"{O3_LEAKAGE_RATE*100:.1f}%",
            "optimizationStrategy": disinfection_strategy,
        },
        "pumpingStation": {
            "waterIntakePumpingStation":   str(round(pg_total, 2)),
            "waterDeliveryPumpingStation": str(round(ssbf_total, 2)),
            "optimizationStrategy":        pumping_strategy,
        },
        "dewateringRoom": {
            "pamConsumption":    str(PAM_KG),
            "sludgeVolume":      str(_sludge["sludge_weight_tons"]) if _sludge else "0",
            "solidContent":      f"{_sludge['sludge_solid_rate']*100:.0f}%" if _sludge else "0%",
            "transportDistance": str(_sludge["sludge_transport_km"]) if _sludge else "0",
            "sludgeTransportCE": str(c_sludge) if _sludge else "0",
            "optimizationStrategy": dewatering_strategy,
        }
    }
}

# ── /api/dashboard/lowcarbon/evaluation  逻辑策略-关联预警 ────────────────────
def _compact_number(value, decimals: int = 1, max_chars: int = 6) -> str:
    """压缩数值表示但不丢失量级，极端大数改用科学计数法。"""
    number = float(value)
    text = f"{number:.{decimals}f}"
    if len(text) <= max_chars:
        return text
    scientific = f"{number:.0e}"
    mantissa, exponent = scientific.split("e")
    compact = f"{mantissa}e{int(exponent)}"
    if len(compact) > max_chars:
        raise ValueError(f"数值无法在{max_chars}字符内完整表达量级：{value}")
    return compact


def _validate_display_lines(lines: list[str], fourth_max: int = 50) -> list[str]:
    """验证显示契约，不通过截断掩盖超长或信息缺失。"""
    result = [str(line).strip() for line in lines[:4]]
    for index, line in enumerate(result):
        max_chars = fourth_max if index == 3 else 25
        if len(line) > max_chars:
            raise ValueError(
                f"逻辑策略第{index + 1}条超过{max_chars}字符：{line}"
            )
    return result


hourly_data = l1_hourly if isinstance(l1_hourly, dict) else {}
peak_h      = hourly_data.get("peak_hour", "N/A")
peak_v      = hourly_data.get("peak_carbon", 0)
valley_h    = hourly_data.get("valley_hour", "N/A")
valley_v    = hourly_data.get("valley_carbon", 0)
above_hours = hourly_data.get("hours_above_avg", [])
day_avg     = hourly_data.get("daily_avg_carbon_per_hour", 0)

if hourly_data.get("available"):
    tou_action = ("建议核查高碳时段设备，确认后再调整排程"
                  if above_hours else "未发现高碳时段，建议维持排程并监测")
    peak_text = _compact_number(peak_v, max_chars=5)
    valley_text = _compact_number(valley_v, max_chars=5)
    avg_text = _compact_number(day_avg, max_chars=5)
    high_hours = "/".join(str(h) for h in above_hours[:3]) + "h" if above_hours else "无"
    tou_lines = [
        f"峰{peak_h}时{peak_text}/谷{valley_h}时{valley_text}kgCO₂e",
        f"均{avg_text}kgCO₂e/h；高{high_hours}",
        tou_action,
    ]
else:
    tou_lines = [
        "峰0时0.0/谷0时0.0kgCO₂e",
        "均0.0kgCO₂e/h；高无",
        "分时碳排正常，建议维持排程并监测",
    ]

pac_lines = []
if isinstance(l2_pac_d, dict) and l2_pac_d:
    pac_lines = [
        f"PAC今日{_compact_number(l2_pac_d.get('pac_unit_today',0), 3)}kg/千吨水",
        f"前3月基准{_compact_number(l2_pac_d.get('pac_baseline_monthly',0), 3)}kg/千吨水",
        f"偏差{l2_pac_d.get('deviation_pct',0):+.1f}%（阈值±20%）",
        "建议检查投加泵校准，优化PAC投加曲线" if "L2" in l3t else "PAC单耗处于正常范围",
    ]

naclo_lines = []
if isinstance(l3_naclo_d, dict) and l3_naclo_d:
    naclo_lines = [
        f"NaClO今日{_compact_number(l3_naclo_d.get('naclo_unit_today',0), 3)}kg/千吨水",
        f"前3月基准{_compact_number(l3_naclo_d.get('naclo_baseline_monthly',0), 3)}kg/千吨水",
        f"偏差{l3_naclo_d.get('deviation_pct',0):+.1f}%（阈值±20%）",
        ("建议核查季节变化与消毒需求，复核NaClO投加参数"
         if "L3" in l3t else "NaClO单耗未触发异常，建议维持当前参数并持续监测"),
    ]

_pac_eval   = ((pac_lines[:3] + [pac_lines[3]])
               if len(pac_lines) >= 4 else pac_lines) or ["PAC单耗未触发异常，建议持续监测"]
_naclo_eval = ((naclo_lines[:3] + [naclo_lines[3]])
                if len(naclo_lines) >= 4 else naclo_lines) or ["NaClO单耗正常，建议维持参数并监测"]

api_evaluation = {
    "code": 0, "msg": "",
    "data": {
        "dayTouCEAnalysis":           _validate_display_lines(tou_lines[:3]),
        "FelCorrPacUcBenchmark":      _validate_display_lines(_pac_eval),
        "wtSeasCorrNaClOUcBenchmark": _validate_display_lines(_naclo_eval),
    }
}

# ── /api/dashboard/lowcarbon/realtime  宏观策略-厂网统筹 ──────────────────────
# 实时卡片读取公共结果的最新行；诊断策略仍使用上面的最新完整日报。
realtime_scope = _row_for_date(scope_daily, "period_start", REALTIME_DATE)
realtime_scope2 = _row_for_date(scope2_daily, "period_start", REALTIME_DATE)
realtime_scope3 = _row_for_date(scope3_daily, "period_start", REALTIME_DATE)
realtime_water = _row_for_date(water_daily, "scope日期", REALTIME_DATE)
realtime_partial = REALTIME_DATE > COMPLETE_REPORT_DATE

elec_carbon = round(_number(realtime_scope2, "plant_total_carbon_kg"), 2)
chem_carbon = round(_number(realtime_scope3, "chemical_carbon_kg"), 2)
realtime_sludge = round(_number(realtime_scope3, "sludge_transport_carbon_kg"), 2)
realtime_o3 = round(_number(realtime_scope, "scope1_carbon_kg"), 3)
scope3_carbon = round(_number(realtime_scope, "scope3_carbon_kg"), 2)
total_carbon = round(_number(realtime_scope, "total_carbon_kg"), 2)
realtime_water_m3 = _number(realtime_water, "水处理量_m3")
unit_carbon = round(total_carbon / realtime_water_m3, 4) if realtime_water_m3 else 0
_pct = (lambda x: round(x / total_carbon * 100, 1) if total_carbon else 0)
source_emissions = {
    "电耗": elec_carbon,
    "药剂": chem_carbon,
    "污泥运输": realtime_sludge,
    "O3泄漏": realtime_o3,
}
dominant_source = max(source_emissions, key=source_emissions.get) if total_carbon else None
if dominant_source == "电耗":
    macro_strategy = "电耗是当前主要碳排来源，建议结合管网压力与供水需求优化厂网泵组调度"
elif dominant_source == "药剂":
    macro_strategy = "药剂是当前主要碳排来源，建议联动原水水质优化投加曲线并复核投加设备效率"
elif dominant_source == "污泥运输":
    macro_strategy = "污泥运输是当前主要碳排来源，建议优化脱水效率、运输批次与车辆调度"
elif dominant_source == "O3泄漏":
    macro_strategy = "O3泄漏是当前主要碳排来源，建议排查密封件和管道接口并优化臭氧投加控制"
else:
    macro_strategy = "当前碳排未触发异常，建议维持现有厂网调度并持续监测"
api_realtime = {
    "code": 0, "msg": "",
    "data": {
        # —— 碳排（kgCO₂e，按范围）——
        "carbonEmissionIntensity":      str(unit_carbon),            # 吨水碳排强度
        "totalCarbonEmissions":         str(total_carbon),           # 全厂总碳排
        "scope1Leakage":                str(realtime_o3),             # 范围1 O3泄漏
        "scope2ElectricityConsumption": str(elec_carbon),            # 范围2电耗
        "scope3Total":                  str(scope3_carbon),          # 范围3合计
        "scope3ChemicalProduction":     str(chem_carbon),            # 范围3药剂生产
        "Scope3SludgeTransportation":   str(realtime_sludge),        # 范围3污泥运输
        # —— 碳排来源构成（占比 %）——
        "electricityConsumption": str(_pct(elec_carbon)),            # 电耗占比
        "chemicalAgent":          str(_pct(chem_carbon)),            # 药剂占比
        "sludgeTransportation":   str(_pct(realtime_sludge)),        # 污泥运输占比
        "O3":                     str(_pct(realtime_o3)),             # O3占比
        "totalWaterSupply":       str(round(realtime_water_m3, 1)),  # 供水总量 m³
        "dataDate":               REALTIME_DATE.isoformat(),
        "periodStatus":           "partial" if realtime_partial else "complete",
        "waterVolumeSource":      volume_source,
        "optimizationStrategy":   macro_strategy,                    # 宏观厂网统筹策略
    }
}

# ── 汇总写入 JSON，key = 接口路径（仅"待对接"接口）────────────────────────────
api_output = {
    "_meta": {
        "date":             REPORT_DATE.strftime("%Y-%m-%d"),
        "plant":            "南沙黄阁水厂",
        "dateSource":       "scope123_总汇总/latest_7d_daily.csv",
        "strategyDate":     REPORT_DATE.strftime("%Y-%m-%d"),
        "realtimeDate":     REALTIME_DATE.isoformat(),
        "realtimePartial":  realtime_partial,
        "hourlyWindowEnd":  LATEST_SHARED_HOUR.isoformat(),
        "periodAligned":    True,
        "dataMode":         "sharedGeneratedOutputs",
        "baselineSource":   "公共月度生成结果",
        "baselineMonths":  shared_baselines["months"],
    },
    # 优化策略诊断
    "/api/dashboard/diagnosis_page?type=1": api_diagnosis_s1,
    "/api/dashboard/diagnosis_page?type=2": api_diagnosis_s2,
    "/api/dashboard/diagnosis_page?type=3": api_diagnosis_s3,
    # 碳排诊断与策略
    "/api/dashboard/lowcarbon/realtime":    api_realtime,
    "/api/dashboard/lowcarbon/strategies":  api_strategies,
    "/api/dashboard/lowcarbon/evaluation":  api_evaluation,
}

# ── 11. 保存接口数据 JSON ─────────────────────────────────────────────────────
# 日期存档版（本地留存）
out_json = BASE / f"南沙黄阁水厂_接口数据_{REPORT_DATE.strftime('%Y%m%d')}.json"
out_json.write_text(json.dumps(api_output, ensure_ascii=False, indent=2), encoding="utf-8")
print(f"接口数据已保存：{out_json.name}")

# 固定文件名版：保留仓库中其他工程师维护的接口，只覆盖本生成器负责的键。
out_json_fixed = BASE / "南沙黄阁水厂_接口数据.json"
repository_interface = API_DATA_DIR / "南沙黄阁水厂_接口数据.json"
merged_output = {}
if repository_interface.is_file():
    merged_output = json.loads(repository_interface.read_text(encoding="utf-8"))
merged_output.update(api_output)
out_json_fixed.write_text(
    json.dumps(merged_output, ensure_ascii=False, indent=2), encoding="utf-8"
)
print(f"接口数据（固定名）：{out_json_fixed.name}")
print("请将此文件复制到仓库 data/ 目录后 git commit & push")
