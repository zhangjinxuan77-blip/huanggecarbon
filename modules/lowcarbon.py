# -*- coding: utf-8 -*-
"""
低碳运行接口
GET /api/dashboard/lowcarbon/realtime   - 宏观策略-厂网统筹（JSON）
GET /api/dashboard/lowcarbon/strategies - 工艺策略-单元优化（JSON）
GET /api/dashboard/lowcarbon/evaluation - 逻辑策略-关联预警（JSON）
数据源：data/南沙黄阁水厂_接口数据.json
"""

import os
from fastapi import APIRouter

from .interface_data import ApiEnvelope, get_interface_response

router = APIRouter()

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "南沙黄阁水厂_接口数据.json")


def _get(key: str) -> dict:
    return get_interface_response(DATA_PATH, key)


@router.get("/api/dashboard/lowcarbon/realtime", response_model=ApiEnvelope)
def lowcarbon_realtime():
    return _get("/api/dashboard/lowcarbon/realtime")


@router.get("/api/dashboard/lowcarbon/strategies", response_model=ApiEnvelope)
def lowcarbon_strategies():
    return _get("/api/dashboard/lowcarbon/strategies")


@router.get("/api/dashboard/lowcarbon/evaluation", response_model=ApiEnvelope)
def lowcarbon_evaluation():
    return _get("/api/dashboard/lowcarbon/evaluation")
