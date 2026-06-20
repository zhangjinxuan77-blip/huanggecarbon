# -*- coding: utf-8 -*-
"""
管网-优化策略接口（报表第57行）
POST /api/network/strategy
数据源：data/管网碳排_按压力监测点_坐标匹配.xlsx（Monthly_PressurePoint）
规则诊断：区域碳排强度离群 / 压力流量失配 / 碳排热点 → diagAnalList + recStrategy
"""

import os
from typing import Dict, Any, List, Tuple

import pandas as pd
from fastapi import APIRouter, HTTPException

router = APIRouter()

BASE_DIR  = os.path.dirname(os.path.dirname(__file__))
XLSX_PATH = os.path.join(BASE_DIR, "data", "管网碳排_按压力监测点_坐标匹配.xlsx")
SHEET     = "Monthly_PressurePoint"   # 优化策略基于月度数据分析

_CACHE: Dict[str, pd.DataFrame] = {}


def _load() -> pd.DataFrame:
    if SHEET in _CACHE:
        return _CACHE[SHEET]
    if not os.path.exists(XLSX_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_excel(XLSX_PATH, sheet_name=SHEET)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取管网数据失败: {repr(e)}")
    _CACHE[SHEET] = df
    return df


def _latest_active(df: pd.DataFrame) -> pd.DataFrame:
    need = ["period", "zone", "point", "CO2e_kg", "flow_m3_h", "pressure_m", "I_kg_m3"]
    if df.empty or any(c not in df.columns for c in need):
        return df.iloc[0:0]
    df = df.copy()
    df["period"] = pd.to_datetime(df["period"], errors="coerce")
    for c in ["CO2e_kg", "flow_m3_h", "pressure_m", "I_kg_m3"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["period", "point"])
    pos = df.loc[df["CO2e_kg"].fillna(0) > 0, "period"]
    if pos.empty:
        return df.iloc[0:0]
    latest = df[df["period"] == pos.max()].copy()
    latest = latest[latest["CO2e_kg"].fillna(0) > 0]
    for c in ["flow_m3_h", "pressure_m", "I_kg_m3"]:
        latest[c] = latest[c].fillna(0.0)
    return latest


def _short(name: str, n: int = 6) -> str:
    s = str(name)
    return s if len(s) <= n else s[:n]


def _diagnose(act: pd.DataFrame) -> Tuple[List[str], str]:
    diag: List[str] = []
    mismatch = False

    # 规则1：区域碳排强度离群
    zi = act.groupby("zone")["I_kg_m3"].mean().sort_values(ascending=False)
    if len(zi) >= 2 and zi.iloc[1] > 0:
        dev = (zi.iloc[0] - zi.iloc[1]) / zi.iloc[1] * 100
        diag.append(f"{_short(zi.index[0],4)}区域碳排强度偏高" if dev > 15 else "各区域碳排强度接近")
    else:
        diag.append("区域碳排强度正常")

    # 规则2：压力流量失配（高压分位 + 低流分位）
    a = act.copy()
    a["pr"] = a["pressure_m"].rank(pct=True)
    a["fr"] = a["flow_m3_h"].rank(pct=True)
    mis = a[(a["pr"] > 0.6) & (a["fr"] < 0.4)]
    if len(mis):
        mismatch = True
        diag.append(f"{_short(mis['point'].iloc[0])}等点高压低流")
    else:
        diag.append("压力与流量匹配正常")

    # 规则3：碳排热点
    hot = act.nlargest(1, "CO2e_kg").iloc[0]
    diag.append(f"{_short(hot['point'])}碳排最高需关注")

    rec = ("建议核查高碳排管段与高压低流点，排查漏损淤积，优化送水泵组压力调度"
           if mismatch else
           "管网整体运行平稳，建议维持现有调度并持续监测高碳排管段")
    return [d[:15] for d in diag[:3]], rec[:40]


@router.post("/api/network/strategy")
def network_strategy() -> Dict[str, Any]:
    act = _latest_active(_load())
    if act.empty:
        return {"code": 0, "msg": "", "data": {
            "diagAnalList": ["暂无管网监测数据"],
            "recStrategy": "暂无数据，待管网监测数据接入后生成优化策略",
        }}
    diag, rec = _diagnose(act)
    return {"code": 0, "msg": "", "data": {"diagAnalList": diag, "recStrategy": rec}}
