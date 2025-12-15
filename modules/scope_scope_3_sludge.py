# -*- coding: utf-8 -*-
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd
from typing import Optional
from modules.common import format_float_2d
# ====== 路由器 ======
router = APIRouter()

# ====== 入参模型 ======
class TimeBody(BaseModel):
    timeType: int  # 1=日, 2=周, 3=月, 4=年


# 前端 timeType → Excel 里的 period
TIME_MAP = {
    1: "日",
    2: "周",
    3: "月",
    4: "年",
}

# ====== 缓存表（注意：这里不用 | None 了） ======
_SLUDGE_TABLE: Optional[pd.DataFrame] = None


def load_sludge_table() -> pd.DataFrame:
    """
    读取《范围1_O3与范围3_污泥运输.xlsx》里 sheet: Scope3_污泥运输
    """
    global _SLUDGE_TABLE
    if _SLUDGE_TABLE is not None:
        return _SLUDGE_TABLE

    try:
        df = pd.read_excel(
            "data/范围1_O3与范围3_污泥运输.xlsx",
            sheet_name="Scope3_污泥运输",
        )
    except Exception as e:
        raise HTTPException(500, f"Excel 加载失败: {e}")

    required_cols = [
        "period",
        "污泥运输碳排放量_kgCO2e",
        "污泥量_吨",
        "含固率_%",
        "运输距离_km",
    ]
    for c in required_cols:
        if c not in df.columns:
            raise HTTPException(500, f"Excel 缺少字段：{c}")

    _SLUDGE_TABLE = df
    return df


# 安全 float 转换：把 '-'、空字符串等都当成 0
def safe_float(v, default: float = 0.0) -> float:
    try:
        if isinstance(v, str):
            s = v.strip()
            if s in ("", "-", "--", "—"):
                return default
            return float(s)
        return float(v)
    except Exception:
        return default


def build_payload(period_label: str) -> dict:
    df = load_sludge_table()
    row = df[df["period"] == period_label]
    if row.empty:
        raise HTTPException(404, f"Excel 未找到周期：{period_label}")

    r = row.iloc[0]

    carbon = safe_float(r["污泥运输碳排放量_kgCO2e"])
    sludge_ton = safe_float(r["污泥量_吨"])
    solids_pct = safe_float(r["含固率_%"])
    dist_km = safe_float(r["运输距离_km"])

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "carbonEmissionsSludgeTransportation": f"{carbon:.6f}",  # 污泥运输碳排
            "chart": [
                ["<div style='color:#F9DA68'>污泥量</div>", f"{sludge_ton:.2f} 吨"],
                ["<div style='color:#F9DA68'>含固率</div>", f"{solids_pct:.2f}%"],
                ["<div style='color:#F9DA68'>运输距离</div>", f"{dist_km:.2f} km"],
            ],
        },
    })


@router.post("/api/scope/scope_3/sludge")
def scope_3_sludge(body: TimeBody):
    period = TIME_MAP.get(int(body.timeType))
    if not period:
        raise HTTPException(400, "timeType 只能是 1(日)/2(周)/3(月)/4(年)")
    return format_float_2d(build_payload(period))
