# -*- coding: utf-8 -*-
"""
工艺段碳排放结构（占比）
接口：/api/process/section/share
入参：{"timeType":1}   # 1=日, 2=周, 3=月, 4=年

读取：
  data/范围2_水厂内外_分段与单元.xlsx
    - Sheet：水厂内_分段
    - Sheet：水厂外_分段

根据 timeType 选择：占比_日 / 占比_周 / 占比_月 / 占比_年
合并相同工艺段（水厂内+水厂外），输出前端要求格式。
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from modules.common import format_float_2d
import pandas as pd
import os

router = APIRouter()


# ========= 入参模型 =========
class TimeBody(BaseModel):
    timeType: int  # 1=日，2=周，3=月，4=年


# ========= 路径配置 =========
APP_DIR = os.path.dirname(os.path.dirname(__file__))
EXCEL_PATH = os.path.join(APP_DIR, "data", "范围2_水厂内外_分段与单元.xlsx")

INNER_SHEET = "水厂内_分段"
OUTER_SHEET = "水厂外_分段"


# ========= 简单缓存 =========
_SHARE_TABLE_CACHE = None


def _load_share_tables():
    """
    读取两个 sheet，并检查必需字段是否存在。
    返回 (df_inner, df_outer)，索引统一为“工艺段”。
    """
    global _SHARE_TABLE_CACHE
    if _SHARE_TABLE_CACHE is not None:
        return _SHARE_TABLE_CACHE

    if not os.path.exists(EXCEL_PATH):
        raise HTTPException(500, f"Excel 文件不存在: {EXCEL_PATH}")

    try:
        df_in = pd.read_excel(EXCEL_PATH, sheet_name=INNER_SHEET)
        df_out = pd.read_excel(EXCEL_PATH, sheet_name=OUTER_SHEET)
    except Exception as e:
        raise HTTPException(500, f"Excel 加载失败: {e}")

    # 必须包含这些字段
    required_cols = ["工艺段", "占比_日", "占比_周", "占比_月", "占比_年"]
    for name, df in [("水厂内_分段", df_in), ("水厂外_分段", df_out)]:
        for c in required_cols:
            if c not in df.columns:
                raise HTTPException(500, f"Sheet「{name}」缺少字段: {c}")

    # 统一用工艺段做索引
    df_in = df_in.set_index("工艺段")
    df_out = df_out.set_index("工艺段")

    _SHARE_TABLE_CACHE = (df_in, df_out)
    return _SHARE_TABLE_CACHE


def _build_share_source(timeType: int):
    """
    根据 timeType 构造占比结构的 source 列表。
    """
    col_map = {
        1: "占比_日",
        2: "占比_周",
        3: "占比_月",
        4: "占比_年",
    }
    col = col_map.get(timeType)
    if not col:
        raise HTTPException(400, "timeType 只能是 1(日)/2(周)/3(月)/4(年)")

    df_in, df_out = _load_share_tables()

    # 收集所有工艺段名称（水厂内 + 水厂外 的并集）
    sections = sorted(set(df_in.index) | set(df_out.index))

    source = []
    for sec in sections:
        val = 0.0
        if sec in df_in.index:
            val += float(df_in.at[sec, col])
        if sec in df_out.index:
            val += float(df_out.at[sec, col])

        source.append({
            "name": str(sec),
            "data": val,  # 已经是占比（百分数），直接给前端
        })

    return source


@router.post("/api/process/section_share")
def process_section_share(body: TimeBody):
    """
    工艺段碳排放结构（占比）
    入参：{"timeType":1}  # 1=日, 2=周, 3=月, 4=年
    """
    try:
        source = _build_share_source(body.timeType)
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
            "dimensionsMapping": ["name", "data"],
        },
    })
