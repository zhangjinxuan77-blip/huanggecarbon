# -*- coding: utf-8 -*-
"""
工艺段碳排量（支持 timeType：日/周/月/年）
读取 《范围2_水厂内外_分段与单元.xlsx》 的：
 - 水厂内_分段
 - 水厂外_分段
按照 timeType 选择：合计_日 / 合计_周 / 合计_月 / 合计_年
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from modules.common import format_float_2d
import pandas as pd
import os

router = APIRouter()

# ========= 入参 =========
class TimeBody(BaseModel):
    timeType: int   # 1=日，2=周，3=月，4=年

# ========= 配置 =========
APP_DIR = os.path.dirname(os.path.dirname(__file__))
EXCEL_PATH = os.path.join(APP_DIR, "data", "范围2_水厂内外_分段与单元.xlsx")

INNER_SHEET = "水厂内_分段"
OUTER_SHEET = "水厂外_分段"

# ========= 缓存 =========
_TABLE_CACHE = None

# ========= 表加载 =========
def _load_tables():
    """读取两个 sheet 并缓存"""
    global _TABLE_CACHE
    if _TABLE_CACHE is not None:
        return _TABLE_CACHE

    if not os.path.exists(EXCEL_PATH):
        raise HTTPException(500, f"Excel 文件不存在: {EXCEL_PATH}")

    try:
        df_in = pd.read_excel(EXCEL_PATH, sheet_name=INNER_SHEET)
        df_out = pd.read_excel(EXCEL_PATH, sheet_name=OUTER_SHEET)
    except Exception as e:
        raise HTTPException(500, f"Excel 加载失败: {e}")

    required_cols = ["工艺段", "合计_日", "合计_周", "合计_月", "合计_年"]
    for name, df in [("水厂内_分段", df_in), ("水厂外_分段", df_out)]:
        for c in required_cols:
            if c not in df.columns:
                raise HTTPException(500, f"Sheet「{name}」缺少字段: {c}")

    df_in = df_in.set_index("工艺段")
    df_out = df_out.set_index("工艺段")

    _TABLE_CACHE = (df_in, df_out)
    return _TABLE_CACHE

# ========= 构造结果 =========
def _build_source(timeType: int):
    """根据 timeType 生成 source 列表"""

    type_map = {
        1: "合计_日",
        2: "合计_周",
        3: "合计_月",
        4: "合计_年"
    }

    col = type_map.get(timeType)
    if not col:
        raise HTTPException(400, "timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    df_in, df_out = _load_tables()
    sections = sorted(set(df_in.index) | set(df_out.index))

    source = []
    for sec in sections:
        v = 0.0
        if sec in df_in.index:
            v += float(df_in.at[sec, col])
        if sec in df_out.index:
            v += float(df_out.at[sec, col])

        source.append({
            "name": str(sec),
            "data": v
        })

    return source

# ========= API =========
@router.post("/api/process/section_total")
def process_section_total(body: TimeBody):
    """
    工艺段碳排量（根据 timeType 返回 日/周/月/年）
    """
    try:
        source = _build_source(body.timeType)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"数据处理失败: {e}")

    return format_float_2d({
        "code": 0,
        "msg": "",
        "data": {
            "dimensions": ["name", "data"],
            "source": source,
            "dimensionsMapping": ["name", "data"]
        }
    })
