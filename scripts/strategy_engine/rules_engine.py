"""Stage 1: Deterministic rule evaluation. All logic lives here — LLM only generates text.

Decision logic source: 优化策略_260508(1).xlsx
Data fields source:    运行参数和对应的字段名(3).xlsx
Formulas source:       公式0620(1).xlsx + 公式调整说明_260511(1).docx
"""

from models import ReportRequest
from typing import Any
import yaml
from pathlib import Path


def load_config() -> dict:
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def _calc_chemical_carbon(cfg: dict, cs, di) -> dict:
    """
    化学品碳排分项验算（有 kg/day 字段时执行）。
    排放因子来源：公式0620(1) 范围3Sheet
      PAC: 0.537 kgCO₂e/kg；PAM: 3.25；NaClO: 0.64；O3投加: 0.28
    """
    ef = cfg["emission_factors"]
    breakdown = {}
    total = 0.0

    if cs.pac_consumption_kg is not None:
        v = cs.pac_consumption_kg * ef["pac"]
        breakdown["pac"] = round(v, 2)
        total += v
    if cs.pam_consumption_kg is not None:
        v = cs.pam_consumption_kg * ef["pam"]
        breakdown["pam"] = round(v, 2)
        total += v
    if di.sodium_hypochlorite_consumption_kg is not None:
        v = di.sodium_hypochlorite_consumption_kg * ef["sodium_hypochlorite"]
        breakdown["sodium_hypochlorite"] = round(v, 2)
        total += v
    if di.ozone_consumption_kg is not None:
        v = di.ozone_consumption_kg * ef["ozone"]
        breakdown["ozone"] = round(v, 2)
        total += v

    if breakdown:
        breakdown["total_calculated"] = round(total, 2)
    return breakdown


def _calc_o3_leakage_carbon(cfg: dict, di, ozone_kg: float) -> float:
    """
    范围1碳排：O3泄漏碳排。
    公式：SCL_O3 × XLL_O3 × GWP_O3（来源：公式0620 范围1碳排Sheet Row3）
    GWP_O3 = 0.04（来源：公式0620 范围1Sheet Row5）
    """
    if di.ozone_leakage_rate is None:
        return 0.0
    return ozone_kg * di.ozone_leakage_rate * cfg["emission_factors"]["ozone_gwp"]


def evaluate(
    request: ReportRequest,
    scada_csv: str | None = None,
    baselines: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    三层诊断引擎。
    Layer 1（宏观）: M1 碳排结构占比波动（4分项）, M2 碳排强度日环比
    Layer 2（单元）: U1 PAC单耗
    Layer 3（逻辑）: L1 分时碳排分析（需传入 scada_csv 路径）

    所有判断逻辑来源：优化策略_260508(1).xlsx
    数据字段来源：运行参数和对应的字段名(3).xlsx
    """
    cfg = load_config()
    t = cfg["thresholds"]
    ref = cfg["reference_values"]
    ef = cfg["emission_factors"]
    defaults = cfg["defaults"]
    baselines = baselines or {}

    plant = request.plant
    units = request.units
    prev = request.previous_plant

    cs = units.coagulation_sedimentation
    di = units.disinfection
    month = plant.timestamp.month

    # ── 供水总量：优先使用上游公共结果传入值，缺失时才使用配置兜底 ───────────
    water_volume_is_default = not (plant.water_volume_m3 and plant.water_volume_m3 > 0)
    water_volume = (plant.water_volume_m3
                    if not water_volume_is_default
                    else defaults["total_water_volume_m3_per_day"])

    # ── O3投加量：优先用传入值，否则使用缺省值 ─────────────────────────────────
    ozone_kg = di.ozone_consumption_kg
    ozone_is_default = False
    if ozone_kg is None:
        ozone_kg = defaults["ozone_consumption_kg_per_day"]
        ozone_is_default = True

    # ── 范围1：O3泄漏碳排 ─────────────────────────────────────────────────────
    # 公式：SCL_O3 × XLL_O3 × GWP_O3（公式0620 范围1Sheet）
    if plant.carbon_emission_o3_leakage is not None:
        carbon_o3_leakage = plant.carbon_emission_o3_leakage
    else:
        carbon_o3_leakage = _calc_o3_leakage_carbon(cfg, di, ozone_kg)

    # ── 污泥运输碳排（范围3）─────────────────────────────────────────────────────
    # 公式：WNL_M × WNHGL_S × YSJL_D × EF_GLYS（公式0620 范围3Sheet Row17）
    sd = units.sludge_dewatering
    carbon_sludge_transport = plant.carbon_emission_sludge_transport or 0.0
    sludge_transport_detail = None
    if (plant.carbon_emission_sludge_transport is None and sd
            and sd.sludge_weight_tons is not None
            and sd.sludge_solid_rate is not None
            and sd.sludge_transport_km is not None):
        ef_transport = ef.get("sludge_transport", 0.12)
        carbon_sludge_transport = sd.sludge_weight_tons * sd.sludge_solid_rate * sd.sludge_transport_km * ef_transport
        carbon_sludge_transport = round(carbon_sludge_transport, 2)
        sludge_transport_detail = {
            "sludge_weight_tons": sd.sludge_weight_tons,
            "sludge_solid_rate": sd.sludge_solid_rate,
            "sludge_transport_km": sd.sludge_transport_km,
            "ef_transport": ef_transport,
            "carbon": carbon_sludge_transport,
        }

    # ── 派生指标 ───────────────────────────────────────────────────────────────
    carbon_scope3 = plant.carbon_emission_chemical + carbon_sludge_transport
    total_carbon = carbon_o3_leakage + plant.carbon_emission_energy + carbon_scope3
    carbon_intensity = total_carbon / water_volume if water_volume > 0 else 0.0

    # 碳排结构占比（M1基础数据，4分项）
    energy_pct = (plant.carbon_emission_energy / total_carbon * 100) if total_carbon > 0 else 0.0
    chemical_pct = (plant.carbon_emission_chemical / total_carbon * 100) if total_carbon > 0 else 0.0
    sludge_transport_pct = (carbon_sludge_transport / total_carbon * 100) if total_carbon > 0 else 0.0
    o3_pct = (carbon_o3_leakage / total_carbon * 100) if total_carbon > 0 else 0.0

    # ── 月度电耗基准：优先使用公共月度结果计算值，缺失时才使用配置兜底 ───────
    energy_carbon_baseline = baselines.get("energy_carbon")
    energy_vs_baseline_pct = None
    energy_baseline_triggered = False
    energy_carbon_list = cfg.get("energy_carbon_monthly_baseline", [None]*12)
    if energy_carbon_baseline is None and len(energy_carbon_list) >= month:
        energy_carbon_baseline = energy_carbon_list[month - 1]
    if energy_carbon_baseline is not None and energy_carbon_baseline > 0:
        energy_vs_baseline_pct = (plant.carbon_emission_energy - energy_carbon_baseline) / energy_carbon_baseline * 100
        energy_baseline_triggered = energy_vs_baseline_pct > t.get("carbon_intensity_delta_pct", 0.1) * 100

    # 化学品碳排分项验算
    chemical_carbon_breakdown = _calc_chemical_carbon(cfg, cs, di)

    # 泵站总电耗
    total_pump_energy = sum(ps.energy_consumption for ps in units.pump_stations)

    # ── Layer 1 — 宏观诊断 ─────────────────────────────────────────────────────
    # 来源：优化策略_260508 Sheet1
    layer1_flags: list[str] = []
    layer1_details: dict[str, Any] = {}

    # M1：碳排结构占比波动 >5%（日环比，需昨日数据）
    # 来源：优化策略 Sheet1 "各碳排类型占比波动>5%触发诊断"
    # 监测4个分项：电耗、药耗、O3泄漏、污泥运输。
    # 与项目其余模块一致，昨日缺失的数值项按0处理，继续完成诊断。
    m1_available = False
    if prev is not None:
        prev_o3 = prev.carbon_emission_o3_leakage or 0.0
        current_components = {
            "energy": plant.carbon_emission_energy,
            "chemical": plant.carbon_emission_chemical,
            "o3": carbon_o3_leakage,
            "sludge": carbon_sludge_transport,
        }
        prev_components = {
            "energy": prev.carbon_emission_energy,
            "chemical": prev.carbon_emission_chemical,
            "o3": prev_o3,
            "sludge": prev.carbon_emission_sludge_transport or 0.0,
        }

        current_gross = sum(current_components.values())
        prev_gross = sum(prev_components.values())
        if prev_gross > 0 and current_gross > 0:
            current_pcts = {k: v / current_gross * 100 for k, v in current_components.items()}
            prev_pcts = {k: v / prev_gross * 100 for k, v in prev_components.items()}
            shifts = {k: abs(current_pcts[k] - prev_pcts[k]) for k in current_components}
            m1_available = True
            threshold_pct = t.get("carbon_structure_shift_pct", 0.05) * 100
            triggered = {k: v for k, v in shifts.items() if v > threshold_pct}
            if triggered:
                layer1_flags.append("carbon_structure_shift")
                layer1_details["M1"] = {
                    "energy_pct_today":    round(current_pcts["energy"], 1),
                    "energy_pct_prev":     round(prev_pcts["energy"], 1),
                    "energy_shift":        round(shifts["energy"], 1),
                    "chemical_pct_today":  round(current_pcts["chemical"], 1),
                    "chemical_pct_prev":   round(prev_pcts["chemical"], 1),
                    "chemical_shift":      round(shifts["chemical"], 1),
                    "o3_pct_today":        round(current_pcts["o3"], 1),
                    "o3_pct_prev":         round(prev_pcts["o3"], 1),
                    "o3_shift":            round(shifts["o3"], 1),
                    "triggered_categories": list(triggered.keys()),
                    "threshold_pct":       threshold_pct,
                }
                layer1_details["M1"].update({
                    "sludge_pct_today": round(current_pcts["sludge"], 1),
                    "sludge_pct_prev": round(prev_pcts["sludge"], 1),
                    "sludge_shift": round(shifts["sludge"], 1),
                })

    # M2：碳排强度日环比 >10%（需昨日数据）
    # 来源：优化策略 Sheet1 "碳排强度上升超过10%触发诊断"
    m2_available = False
    carbon_intensity_delta_pct = 0.0
    prev_intensity = None
    if prev is not None:
        prev_o3_val = prev.carbon_emission_o3_leakage or 0.0
        prev_sludge = prev.carbon_emission_sludge_transport or 0.0
        prev_total = prev_o3_val + prev.carbon_emission_energy + prev.carbon_emission_chemical + prev_sludge
        current_comparable_total = total_carbon
        prev_water_volume = (
            prev.water_volume_m3
            if prev.water_volume_m3 and prev.water_volume_m3 > 0
            else defaults["total_water_volume_m3_per_day"]
        )
        current_comparable_intensity = current_comparable_total / water_volume if water_volume > 0 else 0.0
        _prev_intensity = prev_total / prev_water_volume if prev_water_volume > 0 else 0.0
        if _prev_intensity > 0:
            carbon_intensity_delta_pct = (current_comparable_intensity - _prev_intensity) / _prev_intensity
            m2_available = True
            prev_intensity = _prev_intensity
            if abs(carbon_intensity_delta_pct) > t.get("carbon_intensity_delta_pct", 0.10):
                direction = "上升" if carbon_intensity_delta_pct > 0 else "下降"
                layer1_flags.append(f"carbon_intensity_{direction}")
                layer1_details["M2"] = {
                    "intensity_today": round(current_comparable_intensity, 4),
                    "intensity_prev": round(_prev_intensity, 4),
                    "delta_pct": round(carbon_intensity_delta_pct * 100, 1),
                    "direction": direction,
                    "threshold_pct": t.get("carbon_intensity_delta_pct", 0.10) * 100,
                }


    # ── Layer 2 — 单元诊断 ─────────────────────────────────────────────────────
    # 来源：优化策略_260508 Sheet2
    layer2_flags: dict[str, list[str]] = {
        "coagulation": [],
        "filtration": [],
        "pump_stations": [],
        "sludge_dewatering": [],
    }
    layer2_details: dict[str, Any] = {}

    # U1：PAC单耗（信息类，对比前3月均值，无固定阈值）
    # 来源：优化策略 Sheet2 "混凝沉淀PAC单耗与前3月均值对比"
    # 公式：PAC用量(kg/day) ÷ 供水量(千吨/day)
    pac_unit = None
    if cs.pac_consumption_kg is not None and water_volume > 0:
        pac_unit = cs.pac_consumption_kg / (water_volume / 1000.0)
        pac_ref = ref.get("pac_unit_consumption_3m_avg")
        layer2_details["U1"] = {
            "pac_unit_kg_per_kton": round(pac_unit, 3),
            "pac_3m_avg": pac_ref,
            "vs_avg_pct": round((pac_unit - pac_ref) / pac_ref * 100, 1) if pac_ref else None,
        }

    # O3泄漏率检查
    ozone_leakage_high = False
    if di.ozone_leakage_rate is not None:
        max_leakage = t.get("ozone_leakage_max")
        if max_leakage is not None and di.ozone_leakage_rate > max_leakage:
            ozone_leakage_high = True

    # ── Layer 3 — 逻辑诊断 ────────────────────────────────────────────────────
    # 来源：优化策略_260508 Sheet3
    # L1：分时分析 — 支持公共滚动24小时结果，也兼容原始SCADA CSV
    layer3_rules_triggered: list[str] = []
    layer3_details: dict[str, Any] = {}

    if scada_csv is not None:
        from scada_hourly import analyze_hourly_carbon
        l1_result = analyze_hourly_carbon(
            csv_path=scada_csv,
            target_date=plant.timestamp.date(),
            grid_ef=ef["grid"],
            peak_threshold_pct=t.get("hourly_peak_threshold_pct", 0.20),
        )
        layer3_details["L1"] = l1_result
        if l1_result.get("available") and l1_result.get("hours_above_avg"):
            layer3_rules_triggered.append("L1")

    # L2：PAC单耗与月度季节性基准对比
    # 原水水质信号代理：无在线水质仪表时，PAC单耗偏高是上游负荷升高的最直接可观测指标
    pac_monthly = cfg.get("pac_unit_monthly_baseline", [None] * 12)
    pac_baseline = baselines.get("pac_unit")
    if pac_baseline is None:
        pac_baseline = pac_monthly[month - 1] if len(pac_monthly) >= month else None
    if pac_unit is not None and pac_baseline is not None:
        l2_threshold = cfg.get("l2_pac_deviation_pct", 0.20)
        pac_deviation = (pac_unit - pac_baseline) / pac_baseline
        layer3_details["L2"] = {
            "pac_unit_today": round(pac_unit, 3),
            "pac_baseline_monthly": pac_baseline,
            "deviation_pct": round(pac_deviation * 100, 1),
            "threshold_pct": l2_threshold * 100,
            "month": month,
        }
        if pac_deviation > l2_threshold:
            layer3_rules_triggered.append("L2")

    # L3：NaClO单耗与月度季节性基准对比
    # 水温代理：广州月份与水温强相关；NaClO单耗高于夏季基准说明氯需求异常
    naclo_kg = di.sodium_hypochlorite_consumption_kg
    naclo_monthly = cfg.get("naclo_unit_monthly_baseline", [None] * 12)
    naclo_baseline = baselines.get("naclo_unit")
    if naclo_baseline is None:
        naclo_baseline = naclo_monthly[month - 1] if len(naclo_monthly) >= month else None
    if naclo_kg is not None and naclo_baseline is not None and water_volume > 0:
        naclo_unit = naclo_kg / (water_volume / 1000.0)
        l3_threshold = cfg.get("l3_naclo_deviation_pct", 0.20)
        naclo_deviation = (naclo_unit - naclo_baseline) / naclo_baseline
        layer3_details["L3"] = {
            "naclo_unit_today": round(naclo_unit, 3),
            "naclo_baseline_monthly": naclo_baseline,
            "deviation_pct": round(naclo_deviation * 100, 1),
            "threshold_pct": l3_threshold * 100,
            "month": month,
        }
        if abs(naclo_deviation) > l3_threshold:
            layer3_rules_triggered.append("L3")

    return {
        "layer1_flags": layer1_flags,
        "layer1_details": layer1_details,
        "layer2_flags": layer2_flags,
        "layer2_details": layer2_details,
        "layer3_rules_triggered": layer3_rules_triggered,
        "layer3_details": layer3_details,
        "raw_data_summary": {
            # 供水量
            "water_volume_m3": round(water_volume, 0),
            "water_volume_is_default": water_volume_is_default,
            # 碳强度
            "carbon_intensity": round(carbon_intensity, 4),
            "carbon_intensity_delta_pct": round(carbon_intensity_delta_pct * 100, 1),
            "m1_available": m1_available,
            "m2_available": m2_available,
            "prev_intensity": round(prev_intensity, 4) if prev_intensity is not None else None,
            # 总碳排与结构（4分项；存完整精度，显示层负责四舍五入对齐）
            "total_carbon": round(total_carbon, 2),
            "energy_pct": energy_pct,
            "chemical_pct": chemical_pct,
            "sludge_transport_pct": sludge_transport_pct,
            "o3_pct": o3_pct,
            # 碳排分项（范围1/2/3）
            "carbon_o3_leakage": round(carbon_o3_leakage, 2),
            "carbon_energy": round(plant.carbon_emission_energy, 2),
            "carbon_chemical": round(plant.carbon_emission_chemical, 2),
            "carbon_sludge_transport": round(carbon_sludge_transport, 2),
            "carbon_scope3": round(carbon_scope3, 2),
            "sludge_transport_detail": sludge_transport_detail,
            "energy_carbon_baseline": energy_carbon_baseline,
            "energy_vs_baseline_pct": round(energy_vs_baseline_pct, 1) if energy_vs_baseline_pct is not None else None,
            "energy_baseline_triggered": energy_baseline_triggered,
            # 化学品分项验算
            "chemical_carbon_breakdown": chemical_carbon_breakdown,
            # O3 + NaClO
            "ozone_kg_used": round(ozone_kg, 2),
            "ozone_is_default": ozone_is_default,
            "ozone_leakage_rate": di.ozone_leakage_rate,
            "ozone_leakage_high": ozone_leakage_high,
            "naclo_kg": di.sodium_hypochlorite_consumption_kg,
            # PAC单耗（U1）
            "pac_unit_kg_per_kton": round(pac_unit, 3) if pac_unit is not None else None,
            # 泵站
            "pump_stations": [
                {
                    "station_id": ps.station_id,
                    "energy_consumption": ps.energy_consumption,
                }
                for ps in units.pump_stations
            ],
            "total_pump_energy": round(total_pump_energy, 2),
            # 管段（信息类展示，无诊断逻辑）
            "pipeline_segments": [
                {
                    "segment_id": seg.segment_id,
                    "pressure_drop": seg.pressure_drop,
                    "flow_rate": seg.flow_rate,
                }
                for seg in units.pipeline_segments
            ],
            "grid_emission_factor": ef["grid"],
        },
    }
