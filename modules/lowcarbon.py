# -*- coding: utf-8 -*-
import os
import re
import pandas as pd
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

router = APIRouter()

base_dir = os.path.dirname(os.path.dirname(__file__))
TXT_PATH = os.path.join(base_dir, "data", "碳排诊断输出.txt")


# ======================== 工具函数 ========================

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
    """统一全角符号/破折号/冒号，减少regex失败概率"""
    text = text.replace("：", ":")
    text = text.replace("—", "-").replace("–", "-").replace("－", "-")
    return text


def _head_snippet(text: str, n: int = 400) -> str:
    return text[:n].replace("\n", "\\n").replace("\r", "\\r")


# ==================== 模块1 解析（左：Top5 排名） ====================

def _parse_realtime_top5(text: str):
    """
    兼容：
    - 第1名: XXX - 123 kgCO2e/天
    - 第 1 名：XXX — 123 kgCO2e/日
    - kgCO2e/d, tCO2e/天 等
    """
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


# ==================== 模块2 解析（中：低碳策略） ====================

def _parse_strategies(text: str):
    """
    兼容关键字：
    - 预计降碳 / 预计降碳量 / 预计减排 / 预计减排量 / 年降碳潜力
    单位兼容：
    - kgCO2e/年, tCO2e/年, kgCO2e/yr
    """
    text = _normalize_text(text)
    out = []

    for idx in [1, 2, 3]:
        title_m = re.search(rf"策略\s*{idx}\s*:\s*([^\n\r]+)", text)
        if not title_m:
            raise HTTPException(
                status_code=500,
                detail=f"策略{idx}标题解析失败（未找到 '策略{idx}:' 行）；txt片段={_head_snippet(text)}"
            )

        title = title_m.group(1).strip()

        kg_m = re.search(
            rf"策略\s*{idx}[\s\S]{{0,300}}?(?:预计降碳量?|预计减排量?|预计降碳|预计减排|年降碳潜力)\s*:\s*"
            rf"([\d,]+(?:\.\d+)?)\s*(?:kgCO2e|tCO2e)\s*/\s*(?:年|yr)",
            text,
            flags=re.IGNORECASE
        )
        if not kg_m:
            raise HTTPException(
                status_code=500,
                detail=f"策略{idx}解析失败（预计降碳缺失或单位不匹配）；txt片段={_head_snippet(text)}"
            )

        kg = _must_float(kg_m.group(1))
        out.append({"code": f"策略{idx}", "title": title, "kgco2e_y": kg})

    return out


# ==================== 模块3 解析（右：低碳评估） ====================

def _parse_evaluation(text: str):
    """
    兼容关键字：
    - 绿电碳减排/绿电减排/绿电贡献
    - 总降碳潜力/预计总降碳潜力/年总降碳潜力/预计降碳潜力
    - 绿色低碳等级/绿色等级/低碳等级/等级
    """
    text = _normalize_text(text)

    green_kg = re.search(
        r"(?:绿电碳减排量?|绿电碳减排|绿电减排|绿电贡献)\s*:\s*([\d,]+(?:\.\d+)?)\s*(?:kgCO2e|tCO2e)",
        text, flags=re.IGNORECASE
    )
    total_kg = re.search(
        r"(?:总降碳潜力|预计总降碳潜力|年总降碳潜力|预计降碳潜力)\s*:\s*([\d,]+(?:\.\d+)?)\s*(?:kgCO2e|tCO2e)\s*/\s*(?:年|yr)",
        text, flags=re.IGNORECASE
    )
    grade_m = re.search(
        r"(?:绿色低碳等级|绿色等级|低碳等级|等级)\s*:\s*([^\s\r\n]+)",
        text
    )

    if not (green_kg and total_kg and grade_m):
        raise HTTPException(
            status_code=500,
            detail=f"未能解析到绿色低碳评估关键字段；txt片段={_head_snippet(text)}"
        )

    return {
        "green_kgco2e": _must_float(green_kg.group(1)),
        "total_kgco2e_per_year": _must_float(total_kg.group(1)),
        "grade": grade_m.group(1).strip()
    }


# ======================== 生成 HTML 面板的函数（共用style模板） ========================

def _wrap_panel(title: str, inner_html: str) -> str:
    return f"""
    <div style="
        margin-bottom:36px;
        border:1px solid rgba(42,102,255,.36);
        border-radius:14px;
        padding:22px 22px 18px 22px;
        box-shadow:0 0 18px rgba(42,102,255,.14);
        background:linear-gradient(180deg, rgba(10,18,50,.55), rgba(10,18,50,.32));
        position:relative;
        overflow:hidden;
    ">
        <div style="font-size:20px; font-weight:800; color:#8cc0ff; margin-bottom:12px; text-shadow:0 0 10px rgba(42,102,255,.25);">
            {title}
        </div>
        <div style="height:2px; background:linear-gradient(90deg, #2a66ff, transparent); margin-bottom:18px;"></div>
        {inner_html}
    </div>
    """


# ==================== 左侧模块接口 ====================

@router.get("/api/dashboard/lowcarbon/realtime", response_class=HTMLResponse)
def lowcarbon_realtime():
    text = _read_txt()
    top5 = _parse_realtime_top5(text)

    rows = "".join([f"""
        <div style="
            font-size:16px; padding:7px 0;
            border-left:3px solid rgba(73,179,255,.8);
            padding-left:12px; margin:10px 0;
        ">
            <span style="opacity:.9;">No.{item['rank']}</span>
            <span style="margin-left:6px;">{item['process']}</span>
            <span style="float:right; font-weight:800; color:#cfe2ff;">
                {item['emission']:.2f} kgCO2e/天
            </span>
        </div>
    """ for item in top5])

    conclusion = """
    <div style="
        margin-top:20px;
        padding:16px;
        border:1px solid rgba(79,124,255,0.35);
        border-radius:12px;
        background:rgba(8,16,42,0.35);
        font-size:17px;
        font-weight:900;
        color:#22e36a;
        text-shadow:0 0 10px rgba(34,227,106,0.25);
    ">
        原水提升泵房（因为其排名第一）碳排较高，建议优化泵房运行策略和设备效率
    </div>
    """

    inner_html = rows + conclusion

    html = f"""
    <div style="
        background: linear-gradient(180deg, #0a0f1f, #121a3a);
        min-height: 100vh;
        padding: 40px;
        font-family: 'Inter', sans-serif;
        color: #fff;
    ">
        <h2 style="text-align:center; font-size:26px; font-weight:900; margin-bottom:40px; text-shadow:0 0 10px #2a66ff;">
            实时工艺单元碳排与能耗
        </h2>
        {_wrap_panel("碳排排名（前五）", inner_html)}
    </div>
    """
    return HTMLResponse(html)


# ==================== 中间模块接口 ====================

@router.get("/api/dashboard/lowcarbon/strategies", response_class=HTMLResponse)
def lowcarbon_strategies():
    text = _read_txt()
    strategies = _parse_strategies(text)

    cards = "".join([f"""
      <div style="border:1px solid rgba(79,124,255,.35); border-radius:10px; padding:14px; margin:14px 0; background:rgba(8,16,42,.32);">
        <div style="font-weight:800; font-size:17px; color:#22e36a; text-shadow:0 0 10px rgba(34,227,106,.18);">
            {s['title']}  →  预计降碳 {s['kgco2e_y']:.0f} kgCO2e/年
        </div>
      </div>
    """ for s in strategies])

    html = f"""
    <div style="min-height:100vh; padding:40px; background:linear-gradient(180deg, #07102a, #0b1a44); color:#fff;">
        <h2 style="text-align:center; font-size:26px; font-weight:900; margin-bottom:40px; text-shadow:0 0 10px #2a66ff;">低碳运行策略推荐</h2>
        {cards}
    </div>
    """
    return HTMLResponse(html)


# ==================== 右侧模块接口 ====================

@router.get("/api/dashboard/lowcarbon/evaluation", response_class=HTMLResponse)
def lowcarbon_evaluation():
    text = _read_txt()
    ev = _parse_evaluation(text)

    inner = f"""
        <div style="font-size:18px; padding:8px 0;">碳排年同比变化：<span style="color:#22e36a; font-weight:800;">↑ {ev['grade']}</span></div>
        <div style="font-size:18px; padding:8px 0;">绿电贡献：<span style="font-weight:800;">{ev['green_kgco2e']:.0f} kgCO2e</span></div>
        <div style="font-size:18px; padding:8px 0;">预计降碳潜力：<span style="font-weight:800;">{ev['total_kgco2e_per_year']:.0f} kgCO2e/年</span></div>
        <div style="font-size:20px; padding-top:18px; font-weight:900;">绿色低碳等级：<span style="color:#22e36a; text-shadow:0 0 12px rgba(34,227,106,.25);">{ev['grade']}</span></div>
    """

    html = f"""
    <div style="min-height:100vh; padding:40px; background:linear-gradient(180deg, #07102a, #0b1a44); color:#fff;">
        <h2 style="text-align:center; font-size:26px; font-weight:900; margin-bottom:40px; text-shadow:0 0 10px #2a66ff;">绿色低碳评估</h2>
        {_wrap_panel("评估结果", inner)}
    </div>
    """
    return HTMLResponse(html)
