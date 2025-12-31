# -*- coding: utf-8 -*-
"""
绿色低碳水厂评估 - 前端展示接口
- 左：实时工艺单元碳排与能耗
- 中：低碳运行策略推荐
- 右：绿色低碳评估

默认从 data/绿色低碳评估.txt 解析；解析失败则返回前端示例值（和截图一致）。
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()

# =============== 路径（按你项目结构：routers/xxx.py） ===============
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # project_root/backend/
DATA_DIR = os.path.join(BASE_DIR, "data")
TXT_PATH = os.path.join(DATA_DIR, "绿色低碳评估.txt")


# =============== Pydantic Models ===============
class RealtimeProcessItem(BaseModel):
    name: str
    carbon_kgco2e_per_h: float = Field(..., description="kgCO2e/h")
    energy_kwh: float = Field(..., description="kWh")
    green_power_pct: float = Field(..., description="0-100")
    ts: str = Field(..., description="时间戳字符串，如 2025-08-20 20:35:00")


class StrategyItem(BaseModel):
    code: str = Field(..., description="策略编号，如 策略一")
    title: str = Field(..., description="策略标题")
    reduction_tco2e_per_year: float = Field(..., description="tCO2e/年")


class LowCarbonEvaluation(BaseModel):
    yoy_change_pct: float = Field(..., description="碳排年同比变化（%）")
    green_power_contrib_tco2e: float = Field(..., description="绿电贡献（tCO2e）")
    carbon_sink_contrib: List[str] = Field(..., description="碳汇贡献项")
    reduction_potential_tco2e_per_year: float = Field(..., description="预计降碳潜力（tCO2e/年）")
    grade: str = Field(..., description="绿色低碳等级，如 低/中/高")


class LowCarbonDashboard(BaseModel):
    realtime: List[RealtimeProcessItem]
    strategies: List[StrategyItem]
    evaluation: LowCarbonEvaluation
    hint: str = Field(..., description="一句话提示，用于前端文案")


# =============== 示例值（与你截图对齐） ===============
def _demo_payload() -> LowCarbonDashboard:
    # 截图左侧显示的 3 个单元（示例）
    demo_realtime = [
        RealtimeProcessItem(
            name="预处理",
            carbon_kgco2e_per_h=120.0,
            energy_kwh=500.0,
            green_power_pct=30.0,
            ts="2025-08-20 20:35:00",
        ),
        RealtimeProcessItem(
            name="过滤",
            carbon_kgco2e_per_h=90.0,
            energy_kwh=350.0,
            green_power_pct=25.0,
            ts="2025-08-20 20:35:00",
        ),
        RealtimeProcessItem(
            name="清水池",
            carbon_kgco2e_per_h=60.0,
            energy_kwh=200.0,
            green_power_pct=40.0,
            ts="2025-08-20 20:35:00",
        ),
    ]

    demo_strategies = [
        StrategyItem(code="策略一", title="清水池调蓄提升", reduction_tco2e_per_year=80.0),
        StrategyItem(code="策略二", title="泵站频率优化", reduction_tco2e_per_year=50.0),
        StrategyItem(code="策略三", title="提升光伏自用率", reduction_tco2e_per_year=30.0),
    ]

    demo_eval = LowCarbonEvaluation(
        yoy_change_pct=90.0,
        green_power_contrib_tco2e=150.0,
        carbon_sink_contrib=["光伏", "冷热能", "中水回用"],
        reduction_potential_tco2e_per_year=300.0,
        grade="低",
    )

    return LowCarbonDashboard(
        realtime=demo_realtime,
        strategies=demo_strategies,
        evaluation=demo_eval,
        hint="预处理碳排较高，建议优化投药与能耗",
    )


# =============== 解析 txt（你上传的“绿色低碳评估.txt”风格） ===============
def _read_txt() -> Optional[str]:
    if not os.path.exists(TXT_PATH):
        return None
    try:
        with open(TXT_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None


def _first_match_float(pattern: str, text: str) -> Optional[float]:
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "").strip())
    except Exception:
        return None


def _parse_grade(text: str) -> Optional[str]:
    # 绿色低碳等级: 高
    m = re.search(r"绿色低碳等级[:：]\s*([^\s]+)", text)
    return m.group(1).strip() if m else None


def _parse_green_power_contrib(text: str) -> Optional[float]:
    # 绿电碳减排: 39,520 kgCO2e  -> 转 tCO2e
    kg = _first_match_float(r"绿电碳减排[:：]\s*([\d,\.]+)\s*kg", text)
    if kg is None:
        return None
    return kg / 1000.0


def _parse_reduction_potential(text: str) -> Optional[float]:
    # 总降碳潜力: 7,922,454 kgCO2e/年 -> 转 tCO2e/年
    kg = _first_match_float(r"总降碳潜力[:：]\s*([\d,\.]+)\s*kgCO2e/年", text)
    if kg is None:
        return None
    return kg / 1000.0


def _parse_strategies(text: str) -> List[StrategyItem]:
    # 策略1: 清水池调蓄优化 ... 预计降碳: 2,273,923 kgCO2e/年
    items: List[StrategyItem] = []
    for idx, code in [(1, "策略一"), (2, "策略二"), (3, "策略三")]:
        # 标题（策略X: 后面的内容）
        title_m = re.search(rf"策略{idx}[:：]\s*([^\n\r]+)", text)
        title = title_m.group(1).strip() if title_m else f"策略{idx}"

        # 预计降碳（kgCO2e/年 -> tCO2e/年）
        kg = _first_match_float(rf"策略{idx}.*?预计降碳[:：]\s*([\d,\.]+)\s*kgCO2e/年", text)
        if kg is None:
            # 兜底：有些文本可能写成 “年降碳量”
            kg = _first_match_float(rf"策略{idx}.*?年.*?([\d,\.]+)\s*kgCO2e/年", text)

        if kg is None:
            continue

        items.append(
            StrategyItem(
                code=code,
                title=title.replace("方法:", "").strip(),
                reduction_tco2e_per_year=kg / 1000.0,
            )
        )
    return items


def _build_from_txt(text: str) -> Optional[LowCarbonDashboard]:
    # txt 里通常能解析：策略、总降碳潜力、绿电贡献、等级
    strategies = _parse_strategies(text)
    grade = _parse_grade(text)
    green_contrib = _parse_green_power_contrib(text)
    reduction_potential = _parse_reduction_potential(text)

    # 如果关键字段都拿不到，就认为解析失败
    if not strategies and grade is None and green_contrib is None and reduction_potential is None:
        return None

    # yoy_change_pct / 碳汇贡献：txt 未必有，给合理默认值（前端可照常渲染）
    eval_obj = LowCarbonEvaluation(
        yoy_change_pct=0.0,  # 没有就填 0
        green_power_contrib_tco2e=float(green_contrib or 0.0),
        carbon_sink_contrib=["光伏"],  # 没有就给最保守
        reduction_potential_tco2e_per_year=float(reduction_potential or 0.0),
        grade=grade or "—",
    )

    # realtime：txt 不一定有小时级三段数据 -> 用 demo（你也可以后续换成从 Excel/中台读取）
    demo = _demo_payload()

    hint = "已根据最新诊断结果生成低碳策略与等级"
    return LowCarbonDashboard(
        realtime=demo.realtime,
        strategies=strategies if strategies else demo.strategies,
        evaluation=eval_obj,
        hint=hint,
    )


# =============== 对外接口 ===============
@router.get("/api/dashboard/lowcarbon", response_model=LowCarbonDashboard)
def get_lowcarbon_dashboard():
    """
    一次性返回：左+中+右 三块面板数据（推荐前端用这个接口）
    """
    text = _read_txt()
    if text:
        built = _build_from_txt(text)
        if built:
            return built
    return _demo_payload()


@router.get("/api/dashboard/lowcarbon/realtime", response_model=List[RealtimeProcessItem])
def get_lowcarbon_realtime():
    """
    左侧：实时工艺单元碳排与能耗
    """
    # 目前用 demo；后续你可以改成读 Excel / API / 中台数据
    return _demo_payload().realtime


@router.get("/api/dashboard/lowcarbon/strategies", response_model=List[StrategyItem])
def get_lowcarbon_strategies():
    """
    中间：低碳运行策略推荐
    """
    text = _read_txt()
    if text:
        items = _parse_strategies(text)
        if items:
            return items
    return _demo_payload().strategies


@router.get("/api/dashboard/lowcarbon/evaluation", response_model=LowCarbonEvaluation)
def get_lowcarbon_evaluation():
    """
    右侧：绿色低碳评估
    """
    text = _read_txt()
    if text:
        built = _build_from_txt(text)
        if built:
            return built.evaluation
    return _demo_payload().evaluation
