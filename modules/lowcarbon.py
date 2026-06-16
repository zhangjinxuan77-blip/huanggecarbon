# -*- coding: utf-8 -*-
"""
低碳运行接口
GET /api/dashboard/lowcarbon/realtime   - 宏观策略-厂网统筹（JSON）
GET /api/dashboard/lowcarbon/strategies - 工艺策略-单元优化（JSON）
GET /api/dashboard/lowcarbon/evaluation - 逻辑策略-关联预警（JSON）
数据源：data/南沙黄阁水厂_接口数据.json
"""

import os, json
from fastapi import APIRouter, HTTPException

router = APIRouter()

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "南沙黄阁水厂_接口数据.json")


def _load_json() -> dict:
    if not os.path.exists(DATA_PATH):
        raise HTTPException(status_code=404, detail="未找到接口数据文件")
    with open(DATA_PATH, encoding="utf-8") as f:
        return json.load(f)


def _get(key: str) -> dict:
    data = _load_json()
    if key not in data:
        raise HTTPException(status_code=404, detail=f"数据中不含 {key}")
    return data[key]


@router.get("/api/dashboard/lowcarbon/realtime")
def lowcarbon_realtime():
    return _get("/api/dashboard/lowcarbon/realtime")


@router.get("/api/dashboard/lowcarbon/strategies")
def lowcarbon_strategies():
    return _get("/api/dashboard/lowcarbon/strategies")


@router.get("/api/dashboard/lowcarbon/evaluation")
def lowcarbon_evaluation():
    return _get("/api/dashboard/lowcarbon/evaluation")
