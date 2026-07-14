"""Stage 2: Template-based Chinese report generation. No external API required."""

from models import ReportRequest, Layer2Report, ReportResponse
import yaml
from pathlib import Path


def _load_config() -> dict:
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def generate_report(request: ReportRequest, flags: dict) -> ReportResponse:
    plant = request.plant
    ts = plant.timestamp.strftime("%Y-%m-%d")

    layer1_text = _render_layer1(flags)
    layer2 = _render_layer2(flags)
    layer3_insights = _render_layer3(flags)
    full_text = _render_full_report(plant.plant_id, ts, plant.period, flags, layer3_insights)

    return ReportResponse(
        meta={
            "plant_id": plant.plant_id,
            "timestamp": ts,
            "period": plant.period,
        },
        layer1_text=layer1_text,
        layer2=layer2,
        layer3_insights=layer3_insights,
        full_report_text=full_text,
        flags=flags,
    )


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def _round_pcts(*pcts: float) -> list[float]:
    """四舍五入一组百分比，并将误差补偿到最大项，确保合计精确等于100.0%。"""
    if not pcts or sum(pcts) <= 0:
        return [0.0 for _ in pcts]
    rounded = [round(p, 1) for p in pcts]
    diff = round(100.0 - sum(rounded), 1)
    if diff != 0.0:
        rounded[rounded.index(max(rounded))] = round(rounded[rounded.index(max(rounded))] + diff, 1)
    return rounded


# ── 状态标记 ──────────────────────────────────────────────────────────────────

def _ok(text: str = "正常") -> str:
    return f"✓ {text}"

def _warn(text: str) -> str:
    return f"⚠ {text}"


def _energy_efficiency_strategy(summary: dict) -> str:
    """按当前电耗最高的站点生成排查建议。"""
    stations = [
        station
        for station in summary.get("pump_stations", [])
        if station.get("energy_consumption", 0) > 0
    ]
    if not stations:
        return "建议核查全厂高耗能设备运行效率并优化调度。"
    hotspot = max(stations, key=lambda station: station["energy_consumption"])
    return f"建议优先核查{hotspot['station_id']}运行效率并优化设备调度。"


# ── Layer 1：全厂摘要 ─────────────────────────────────────────────────────────

def _render_layer1(flags: dict) -> str:
    s = flags["raw_data_summary"]
    l1 = flags["layer1_flags"]
    l1d = flags["layer1_details"]

    intensity = s["carbon_intensity"]
    total_carbon = s["total_carbon"]
    energy_pct, chemical_pct, sludge_pct, o3_pct_r = _round_pcts(
        s["energy_pct"], s["chemical_pct"], s["sludge_transport_pct"], s["o3_pct"]
    )
    component_pcts = {
        "能耗": s["energy_pct"],
        "药耗": s["chemical_pct"],
        "污泥运输": s["sludge_transport_pct"],
        "O3": s["o3_pct"],
    }
    dominant_cn = max(component_pcts, key=component_pcts.get) if total_carbon > 0 else None
    dominant_text = f"{dominant_cn}主导" if dominant_cn else "暂无主导来源"

    # M2：碳排强度日环比
    if not s["m2_available"]:
        trend = "（无昨日数据，暂无日环比）"
    elif "carbon_intensity_上升" in l1:
        delta = abs(s["carbon_intensity_delta_pct"])
        trend = _warn(f"较昨日上升{delta:.1f}%")
    elif l1d.get("M2", {}).get("direction") == "下降":
        delta = abs(s["carbon_intensity_delta_pct"])
        trend = f"较昨日下降{delta:.1f}%"
    else:
        delta = s["carbon_intensity_delta_pct"]
        trend = f"与昨日基本持平（{delta:+.1f}%）"

    # M1：碳排结构占比波动（4分项）
    if not s["m1_available"]:
        structure_status = (
            f"电耗{energy_pct:.1f}%，药耗{chemical_pct:.1f}%，"
            f"污泥运输{sludge_pct:.1f}%，O3{o3_pct_r:.1f}%（无昨日数据）"
        )
    elif "carbon_structure_shift" in l1:
        d = l1d.get("M1", {})
        shift_parts = []
        for cat, cn in [("energy", "电耗"), ("chemical", "药耗"), ("sludge", "污泥运输"), ("o3", "O3")]:
            if cat in d.get("triggered_categories", []):
                shift_parts.append(
                    _warn(f"{cn}{d.get(f'{cat}_pct_today', 0):.1f}%→昨日{d.get(f'{cat}_pct_prev', 0):.1f}%（Δ{d.get(f'{cat}_shift', 0):.1f}%）")
                )
        structure_status = "碳排结构波动：" + "；".join(shift_parts)
    else:
        structure_status = (
            f"碳排结构稳定（电耗{energy_pct:.1f}%，药耗{chemical_pct:.1f}%，"
            f"污泥运输{sludge_pct:.1f}%，O3{o3_pct_r:.1f}%）"
        )

    return (
        f"吨水碳排强度{intensity:.3f} kgCO₂e/m³，{trend}。"
        f"全厂总碳排{total_carbon:.0f} kgCO₂e，{dominant_text}（{structure_status}）。"
    )


# ── Layer 2：各工艺单元 ───────────────────────────────────────────────────────

def _render_layer2(flags: dict) -> Layer2Report:
    return Layer2Report(
        coagulation=_coagulation(flags),
        filtration=None,
        disinfection=_disinfection(flags),
        pump_stations=_pump_stations(flags),
        pipeline_segments=_pipeline_segments(flags),
        sludge_dewatering=_sludge_dewatering(flags),
    )


def _coagulation(flags: dict) -> str:
    s = flags["raw_data_summary"]
    l2d = flags["layer2_details"]

    # U1：PAC单耗信息
    u1 = l2d.get("U1")
    if u1:
        pac_unit = u1["pac_unit_kg_per_kton"]
        vs = u1.get("vs_avg_pct")
        if vs is not None:
            if vs > 0:
                pac_status = f"PAC单耗{pac_unit:.3f} kg/千吨水（较3月均值+{vs:.1f}%）"
            else:
                pac_status = f"PAC单耗{pac_unit:.3f} kg/千吨水（较3月均值{vs:.1f}%）"
        elif u1.get("pac_3m_avg") is not None:
            pac_status = f"PAC单耗{pac_unit:.3f} kg/千吨水（历史均值无有效正值，暂不比较）"
        else:
            pac_status = f"PAC单耗{pac_unit:.3f} kg/千吨水（历史均值未标定）"
    else:
        pac_status = "PAC用量数据未提供"

    return pac_status + "。"


def _filtration(flags: dict) -> str:
    return "当前公共结果未提供反冲洗泵与滤速指标，暂不做过滤单元策略判断。"


def _disinfection(flags: dict) -> str:
    s = flags["raw_data_summary"]
    ozone_rate = s["ozone_leakage_rate"]

    parts = []
    if ozone_rate is not None:
        if s["ozone_leakage_high"]:
            parts.append(_warn(f"O3泄漏率{ozone_rate*100:.1f}%，超出阈值5%"))
        else:
            parts.append(_ok(f"O3泄漏率{ozone_rate*100:.1f}%"))
    else:
        parts.append("O3泄漏率数据未提供")

    return "；".join(parts) + "。"


def _pump_stations(flags: dict) -> str:
    s = flags["raw_data_summary"]

    parts = []
    for ps in s["pump_stations"]:
        parts.append(f"{ps['station_id']}电耗{ps['energy_consumption']:,.0f} kWh")

    parts.append(f"合计{s['total_pump_energy']:.0f} kWh")
    return "；".join(parts) + "。"


def _pipeline_segments(flags: dict) -> str:
    s = flags["raw_data_summary"]
    segments = s.get("pipeline_segments", [])

    if not segments:
        return "无管段数据。"

    parts = []
    for seg in segments:
        parts.append(f"{seg['segment_id']}压降{seg['pressure_drop']:.3f} MPa（{seg['flow_rate']:,.0f} m³/h）")

    return "；".join(parts) + "。"


def _sludge_dewatering(flags: dict) -> str:
    s = flags["raw_data_summary"]
    breakdown = s.get("chemical_carbon_breakdown", {})
    pam_carbon = breakdown.get("pam")
    if pam_carbon is not None:
        pam_kg = round(pam_carbon / 3.25, 1)
        return f"PAM药耗{pam_kg:.1f} kg/day。"
    return None


def _render_l2_pac(d: dict) -> str:
    dev = d["deviation_pct"]
    baseline_label = d.get("baseline_label", f"{d['month']}月季节基准")
    triggered = d.get("triggered", abs(dev) > d["threshold_pct"])
    direction = "高于" if dev > 0 else "低于"
    sign = "+" if dev > 0 else ""
    if triggered and dev > 0:
        conclusion = _warn("PAC单耗显著偏高")
        strategy  = "  → 建议检查混凝剂投加泵校准状态，复核混合搅拌器运行效率，并结合原水浊度优化PAC投加曲线。"
    elif triggered and dev < 0:
        conclusion = _ok("PAC单耗低于季节基准")
        strategy  = None
    else:
        conclusion = _ok(f"PAC单耗在季节基准范围内（偏差{sign}{dev:.1f}%）")
        strategy  = None
    lines = [
        f"前端负荷关联 — PAC单耗{direction}{baseline_label}",
        f"  今日PAC单耗  {d['pac_unit_today']:.3f} kg/千吨水",
        f"  {baseline_label}  {d['pac_baseline_monthly']:.3f} kg/千吨水",
        f"  偏差  {sign}{dev:.1f}%（阈值±{d['threshold_pct']:.0f}%）",
        f"  {conclusion}",
    ]
    if strategy:
        lines.append(strategy)
    return "\n".join(lines)


def _render_l3_naclo(d: dict) -> str:
    dev = d["deviation_pct"]
    baseline_label = d.get("baseline_label", f"{d['month']}月季节基准")
    triggered = d.get("triggered", abs(dev) > d["threshold_pct"])
    direction = "高于" if dev > 0 else "低于"
    sign = "+" if dev > 0 else ""
    if triggered:
        conclusion = _warn(f"NaClO单耗显著偏{'高' if dev > 0 else '低'}")
        strategy   = "  → 建议根据水温季节变化动态调整工艺参数预设值。"
    else:
        conclusion = _ok(f"NaClO单耗在季节基准范围内（偏差{sign}{dev:.1f}%）")
        strategy   = "  → 建议根据水温季节变化动态调整工艺参数预设值。"
    lines = [
        f"水温季节相关性 — NaClO单耗{direction}{baseline_label}",
        f"  今日NaClO单耗  {d['naclo_unit_today']:.3f} kg/千吨水",
        f"  {baseline_label}  {d['naclo_baseline_monthly']:.3f} kg/千吨水",
        f"  偏差  {sign}{dev:.1f}%（阈值±{d['threshold_pct']:.0f}%）",
        f"  {conclusion}",
        strategy,
    ]
    return "\n".join(lines)


# ── Layer 3：逻辑层说明 ───────────────────────────────────────────────────────

def _render_layer3(flags: dict) -> list[str]:
    l3d = flags["layer3_details"]
    triggered = set(flags["layer3_rules_triggered"])
    insights = []

    # L1 仅在触发时显示（需要SCADA CSV）
    if "L1" in triggered:
        detail = l3d.get("L1", {})
        if isinstance(detail, dict) and detail.get("available"):
            insights.append(_render_l1_hourly(detail))

    # L2/L3 只要有数据就显示（触发或正常都说明）
    for rule, renderer in [("L2", _render_l2_pac), ("L3", _render_l3_naclo)]:
        detail = l3d.get(rule)
        if isinstance(detail, dict):
            detail["triggered"] = rule in triggered
            insights.append(renderer(detail))

    return insights


def _render_l1_hourly(d: dict) -> str:
    hourly = d["hourly_carbon"]
    peak_h = d["peak_hour"]
    valley_h = d["valley_hour"]
    ratio = d["peak_valley_ratio"]
    above = d["hours_above_avg"]
    avg = d["daily_avg_carbon_per_hour"]
    threshold = d["peak_threshold_pct"]

    above_str = (
        "、".join(f"{h:02d}:00" for h in above)
        if above else "无"
    )

    lines = [
        "日间分时碳排分析",
        f"  峰值  {peak_h:02d}:00  {d['peak_carbon']:.1f} kgCO₂e"
        f"  谷值  {valley_h:02d}:00  {d['valley_carbon']:.1f} kgCO₂e"
        f"  峰谷比  {ratio:.2f}x",
        f"  日均  {avg:.1f} kgCO₂e/h    超日均{threshold:.0f}%的小时：{above_str}",
        "  逐小时碳排（kgCO₂e）：",
    ]
    if above:
        lines.insert(2, "  → 建议核查是否可安排部分操作（如反冲洗或排泥）转移至低谷时段。")

    # 24小时柱状展示（每行8小时）
    for row_start in range(0, 24, 8):
        hour_labels = "  ".join(f"{h:02d}h" for h in range(row_start, row_start + 8))
        values = "  ".join(f"{hourly[h]:5.1f}" for h in range(row_start, row_start + 8))
        lines.append(f"  {hour_labels}")
        lines.append(f"  {values}")

    return "\n".join(lines)


# ── 完整报告文本 ──────────────────────────────────────────────────────────────

def _render_full_report(
    plant_id: str, date: str, period: str,
    flags: dict, insights: list[str]
) -> str:
    s = flags["raw_data_summary"]
    l1 = flags["layer1_flags"]
    l1d = flags["layer1_details"]
    l2 = flags["layer2_flags"]
    l2d = flags["layer2_details"]
    cfg = _load_config()
    ref = cfg["reference_values"]

    W = 52
    div = "─" * W

    # ── 第一部分：全厂指标 ───────────────────────────────────────────────────

    intensity = s["carbon_intensity"]
    delta_pct = s["carbon_intensity_delta_pct"]
    has_sludge = s.get("carbon_sludge_transport", 0) > 0
    if has_sludge:
        energy_pct, chemical_pct, sludge_pct, o3_pct_s = _round_pcts(
            s["energy_pct"], s["chemical_pct"], s["sludge_transport_pct"], s["o3_pct"]
        )
    else:
        energy_pct, chemical_pct, o3_pct_s = _round_pcts(
            s["energy_pct"], s["chemical_pct"], s["o3_pct"]
        )
        sludge_pct = 0.0

    def _structure_str(e, c, sl, o3, suffix=""):
        parts = f"电耗{e:.1f}%  药剂{c:.1f}%"
        if sl > 0:
            parts += f"  污泥运输{sl:.1f}%"
        parts += f"  O3{o3:.1f}%"
        return parts + suffix

    if not s["m2_available"]:
        trend_str = "→ 无昨日数据"
    elif "carbon_intensity_上升" in l1:
        trend_str = f"⚠ 较昨日 +{abs(delta_pct):.1f}%"
    elif l1d.get("M2", {}).get("direction") == "下降":
        trend_str = f"↓ 较昨日 -{abs(delta_pct):.1f}%"
    else:
        trend_str = f"→ 较昨日 {delta_pct:+.1f}%"

    if not s["m1_available"]:
        structure_str = _structure_str(energy_pct, chemical_pct, sludge_pct, o3_pct_s, "（无昨日数据）")
    elif "carbon_structure_shift" in l1:
        d = l1d.get("M1", {})
        parts = []
        for cat, cn in [("energy", "电耗"), ("chemical", "药剂"), ("sludge", "污泥运输"), ("o3", "O3")]:
            if cat in d.get("triggered_categories", []):
                parts.append(f"⚠ {cn}{d.get(f'{cat}_pct_today', 0):.1f}%→昨{d.get(f'{cat}_pct_prev', 0):.1f}%（Δ{d.get(f'{cat}_shift', 0):.1f}%）")
        structure_str = "结构波动：" + "  ".join(parts)
    else:
        structure_str = "稳定（" + _structure_str(energy_pct, chemical_pct, sludge_pct, o3_pct_s) + "）"

    if has_sludge:
        scope3_lines = [
            f"    ├ 范围3 药剂生产  {s['carbon_chemical']:,.0f} kgCO₂e",
            f"    └ 范围3 污泥运输  {s['carbon_sludge_transport']:,.0f} kgCO₂e",
        ]
        scope3_label = f"    ├ 范围3 合计     {s['carbon_scope3']:,.0f} kgCO₂e"
    else:
        scope3_lines = [f"    └ 范围3 药剂生产  {s['carbon_chemical']:,.0f} kgCO₂e"]
        scope3_label = None

    scope3_block = ([scope3_label] if scope3_label else []) + scope3_lines if has_sludge else scope3_lines

    section1_lines = [
        f"  {'吨水碳排强度':<10}{intensity:.4f} kgCO₂e/m³    {trend_str}",
        f"  {'全厂总碳排':<10}{s['total_carbon']:,.0f} kgCO₂e",
        f"    ├ 范围1 O3泄漏   {s['carbon_o3_leakage']:,.2f} kgCO₂e",
        f"    ├ 范围2 电耗     {s['carbon_energy']:,.0f} kgCO₂e",
    ] + scope3_block + [
        f"  {structure_str}",
        f"  {'供水总量':<10}{s['water_volume_m3']:,.0f} m³",
    ]
    # ── 一层策略建议 ─────────────────────────────────────────────────────────
    strategy1_lines = []
    if "carbon_structure_shift" in l1:
        strategy1_lines.append("  → 建议关注药剂投加系统运行状态，复核投加量或药剂类型。")
        strategy1_lines.append("  → 建议评估光伏系统发电效率，或考虑增加其他可再生能源。")
    if "carbon_intensity_上升" in l1 or s.get("energy_baseline_triggered"):
        strategy1_lines.append(f"  → {_energy_efficiency_strategy(s)}")
    if strategy1_lines:
        section1_lines += [""] + strategy1_lines

    section1 = "\n".join(section1_lines)

    # ── 第二部分：混凝沉淀 ───────────────────────────────────────────────────

    u1 = l2d.get("U1")
    l3d = flags.get("layer3_details", {})
    if u1:
        pac_unit = u1["pac_unit_kg_per_kton"]
        pac_ref = u1["pac_3m_avg"]
        coag_lines = [f"  {'PAC单耗':<14}{pac_unit:.3f} kg/千吨水"]
        pac_high = (pac_ref is not None and pac_unit > pac_ref * 1.10)
        l2_detail = l3d.get("L2", {})
        if not pac_high and isinstance(l2_detail, dict):
            pac_high = l2_detail.get("triggered") and l2_detail.get("deviation_pct", 0) > 0
        if pac_high:
            coag_lines.append(f"  {_warn('PAC单耗偏高')}")
            coag_lines.append("  → 建议检查混凝剂投加泵校准状态，复核混合搅拌器运行效率。结合原水浊度变化，优化PAC投加曲线。")
        else:
            coag_lines.append(f"  {_ok('混凝运行正常')}")
    else:
        coag_lines = [f"  {'PAC用量':<14}数据未提供"]

    section_coag = "\n".join(coag_lines)

    # ── 第三部分：过滤 ───────────────────────────────────────────────────────

    section_filt = "  过滤单元      公共结果暂无反冲洗泵与滤速指标，未执行策略判断"

    # ── 第四部分：消毒 ───────────────────────────────────────────────────────

    ozone_rate   = s["ozone_leakage_rate"]
    naclo_kg_dis = s.get("naclo_kg")
    water_vol    = s.get("water_volume_m3", 346536)
    dis_lines    = []

    if ozone_rate is not None:
        o3_high  = s["ozone_leakage_high"]
        o3_mark  = _warn("超出阈值5%") if o3_high else _ok()
        dis_lines.append(f"  {'O3泄漏率':<14}{ozone_rate*100:.1f}%    {o3_mark}")
    else:
        dis_lines.append(f"  {'O3泄漏率':<14}数据未提供")

    if naclo_kg_dis is not None and water_vol:
        naclo_unit_kton = naclo_kg_dis / (water_vol / 1000)
        l3_naclo = flags.get("layer3_details", {}).get("L3")
        if l3_naclo and isinstance(l3_naclo, dict):
            dev = l3_naclo.get("deviation_pct", 0)
            if abs(dev) > l3_naclo.get("threshold_pct", 20):
                naclo_status = _warn(f"NaClO单耗偏{'高' if dev>0 else '低'} {dev:+.1f}%")
            else:
                naclo_status = _ok("NaClO单耗正常")
            dis_lines.append(f"  {'NaClO单耗':<13}{naclo_unit_kton:.3f} kg/千吨水    {naclo_status}")

    section_dis = "\n".join(dis_lines)

    # ── 第五部分：泵站 ───────────────────────────────────────────────────────

    pump_lines = []
    for ps in s["pump_stations"]:
        pump_lines.append(f"  {ps['station_id']:<16}{ps['energy_consumption']:,.0f} kWh")
    pump_lines.append(f"  {'合计':<16}{s['total_pump_energy']:,.0f} kWh")
    if water_vol and s["total_pump_energy"] > 0:
        unit_e = s["total_pump_energy"] / (water_vol / 1000)
        pump_lines.append(f"  {'泵站单耗':<16}{unit_e:.3f} kWh/千吨水")
    pump_lines.append(f"  {_ok('泵站运行正常')}")
    section_pump = "\n".join(pump_lines)

    # ── 第六部分：管网 ───────────────────────────────────────────────────────

    segments = s.get("pipeline_segments", [])
    if segments:
        seg_lines = [
            f"  {seg['segment_id']:<14}压降{seg['pressure_drop']:.3f} MPa    {seg['flow_rate']:,.0f} m³/h"
            for seg in segments
        ]
        section_seg = "\n".join(seg_lines)
    else:
        section_seg = "  无管段数据。"

    # ── 第七部分：脱水间 ─────────────────────────────────────────────────────

    breakdown = s.get("chemical_carbon_breakdown", {})
    pam_carbon = breakdown.get("pam")
    dw_lines = []
    if pam_carbon is not None:
        pam_kg = round(pam_carbon / 3.25, 1)
        dw_lines.append(f"  {'PAM用量':<16}{pam_kg:.1f} kg/day")
    st = s.get("sludge_transport_detail")
    if st:
        sludge_t = st["sludge_weight_tons"]
        solid_r  = st["sludge_solid_rate"]
        dw_lines.append(f"  {'污泥量':<16}{sludge_t:.2f} 吨/day")
        dw_lines.append(f"  {'含固率':<16}{solid_r*100:.2f}%")
        dw_lines.append(f"  {'运输距离':<14}{st['sludge_transport_km']:.2f} km")
        dw_lines.append(f"  {'污泥运输碳排':<12}{st['carbon']:,.2f} kgCO₂e")
        # PAM单耗（kg/吨干基）
        if pam_carbon is not None and sludge_t > 0 and solid_r > 0:
            dry_mass = sludge_t * solid_r
            if dry_mass > 0:
                pam_per_dry = round(pam_carbon / 3.25, 1) / dry_mass
                dw_lines.append(f"  {'PAM单耗':<16}{pam_per_dry:.3f} kg/吨干基")
        # PAM单耗状态（暂无历史基准，默认正常）
        dw_lines.append(f"  {_ok('脱水运行正常')}")
    else:
        dw_lines.append(f"  {_ok('脱水运行正常')}")
    section_dw = "\n".join(dw_lines) if dw_lines else None

    # ── 拼装 ─────────────────────────────────────────────────────────────────

    lines = [
        f"{plant_id}  运行分析报告",
        f"日期：{date}    报告类型：{period}",
        div,
        "",
        "一、全厂关键指标",
        section1,
        "",
        div,
        "二、工艺单元状态",
        "",
        "  ▌ 混凝沉淀",
        section_coag,
        "",
        "  ▌ 过滤",
        section_filt,
        "",
        "  ▌ 消毒",
        section_dis,
        "",
        "  ▌ 泵站",
        section_pump,
    ]

    if segments:
        lines += ["", "  ▌ 管网", section_seg]

    lines += ["", "  ▌ 脱水间", section_dw if section_dw else "  暂无数据"]
    lines += ["", div]

    lines += ["三、逻辑层诊断", ""]
    if insights:
        indented = []
        for insight in insights:
            indented.append("\n".join("  " + ln for ln in insight.split("\n")))
        lines += ["\n\n".join(indented)]
    else:
        lines += ["  ✓ 逻辑层诊断无异常，各项指标运行正常"]
    lines += ["", div]

    return "\n".join(lines)
