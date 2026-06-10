# -*- coding: utf-8 -*-
"""
低碳运行接口
GET /api/dashboard/lowcarbon/realtime   - 实时工艺单元碳排（保留原 TXT 实现）
GET /api/dashboard/lowcarbon/strategies - 低碳策略推荐（JSON）
GET /api/dashboard/lowcarbon/evaluation - 绿色低碳评估（JSON）
数据源：data/南沙黄阁水厂_接口数据.json（strategies / evaluation）
       data/绿色低碳评估.txt（realtime）
"""

import os, re, json
import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

router = APIRouter()

BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TXT_PATH      = os.path.join(BASE_DIR, "data", "绿色低碳评估.txt")
DATA_PATH     = os.path.join(BASE_DIR, "data", "南沙黄阁水厂_接口数据.json")


# ── JSON 数据加载 ─────────────────────────────────────────────────────────────

def _load_json() -> dict:
    if not os.path.exists(DATA_PATH):
        raise HTTPException(status_code=404, detail="未找到接口数据文件")
    with open(DATA_PATH, encoding="utf-8") as f:
        return json.load(f)


# ── TXT 工具函数（realtime 用）────────────────────────────────────────────────

def _read_txt() -> str:
    if not os.path.exists(TXT_PATH):
        raise HTTPException(status_code=404, detail=f"未找到诊断文件: {TXT_PATH}")
    try:
        with open(TXT_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取诊断文件失败: {e}")


def _must_float(s: str) -> float:
    try:
        return float(str(s).replace(",", "").strip())
    except Exception:
        raise HTTPException(status_code=500, detail=f"数值解析失败: {s}")


def _normalize_text(text: str) -> str:
    text = text.replace("：", ":")
    text = text.replace("—", "-").replace("–", "-").replace("－", "-")
    return text


def _head_snippet(text: str, n: int = 400) -> str:
    return text[:n].replace("\n", "\\n").replace("\r", "\\r")


def _parse_realtime_top5(text: str):
    text = _normalize_text(text)
    pattern = re.compile(
        r"第\s*(\d+)\s*名\s*:\s*([^\n\r\-]+?)\s*-\s*([\d,]+(?:\.\d+)?)\s*"
        r"(?:kgCO2e|tCO2e)\s*/\s*(?:天|日|d)",
        flags=re.IGNORECASE
    )
    matches = pattern.findall(text)
    if not matches:
        raise HTTPException(
            status_code=500,
            detail=f"未能解析到实时工艺单元碳排排名数据；txt片段={_head_snippet(text)}"
        )
    df = pd.DataFrame(matches, columns=["rank", "process", "emission"])
    df["rank"] = df["rank"].astype(int)
    df["emission"] = df["emission"].map(_must_float)
    df = df.sort_values("rank").head(5)
    return df.to_dict(orient="records")


def _wrap_panel(title: str, inner_html: str) -> str:
    return f"""
    <div style="
        margin-bottom:36px;
        border:1px solid rgba(42,102,255,.36);
        border-radius:14px;
        padding:22px 22px 18px 22px;
        box-shadow:0 0 18px rgba(42,102,255,.14);
        background:linear-gradient(180deg, rgba(10,18,50,.55), rgba(10,18,50,.32));
    ">
        <div style="font-size:20px; font-weight:800; color:#8cc0ff; margin-bottom:12px;">{title}</div>
        <div style="height:2px; background:linear-gradient(90deg, #2a66ff, transparent); margin-bottom:18px;"></div>
        {inner_html}
    </div>
    """


# ── 接口 ──────────────────────────────────────────────────────────────────────

@router.get("/api/dashboard/lowcarbon/realtime", response_class=HTMLResponse)
def lowcarbon_realtime():
    text = _read_txt()
    top5 = _parse_realtime_top5(text)
    rows = "".join([f"""
        <div style="font-size:16px; padding:7px 0; border-left:3px solid rgba(73,179,255,.8); padding-left:12px; margin:10px 0;">
            <span style="opacity:.9;">No.{item['rank']}</span>
            <span style="margin-left:6px;">{item['process']}</span>
            <span style="float:right; font-weight:800; color:#cfe2ff;">{item['emission']:.2f} kgCO2e/天</span>
        </div>
    """ for item in top5])
    conclusion = """
    <div style="margin-top:20px; padding:16px; border:1px solid rgba(79,124,255,0.35); border-radius:12px;
        background:rgba(8,16,42,0.35); font-size:17px; font-weight:900; color:#22e36a;">
        原水提升泵房（因为其排名第一）碳排较高，建议优化泵房运行策略和设备效率
    </div>
    """
    html = f"""
    <div style="background:linear-gradient(180deg,#0a0f1f,#121a3a); min-height:100vh; padding:40px; color:#fff;">
        <h2 style="text-align:center; font-size:26px; font-weight:900; margin-bottom:40px;">实时工艺单元碳排与能耗</h2>
        {_wrap_panel("碳排排名（前五）", rows + conclusion)}
    </div>
    """
    return HTMLResponse(html)


@router.get("/api/dashboard/lowcarbon/strategies")
def lowcarbon_strategies():
    data = _load_json()
    key  = "/api/dashboard/lowcarbon/strategies"
    if key not in data:
        raise HTTPException(status_code=404, detail="数据中不含 strategies")
    return data[key]


@router.get("/api/dashboard/lowcarbon/evaluation")
def lowcarbon_evaluation():
    data = _load_json()
    key  = "/api/dashboard/lowcarbon/evaluation"
    if key not in data:
        raise HTTPException(status_code=404, detail="数据中不含 evaluation")
    return data[key]
