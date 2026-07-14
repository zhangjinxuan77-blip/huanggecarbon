# -*- coding: utf-8 -*-
"""
优化策略诊断接口
GET /api/dashboard/diagnosis_page?type=1|2|3
数据源：data/南沙黄阁水厂_接口数据.json
"""

import os
from fastapi import APIRouter, Query

from .interface_data import ApiEnvelope, get_interface_response

router = APIRouter()

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "南沙黄阁水厂_接口数据.json")
FIXED_KEY = {
    1: "/api/dashboard/diagnosis_page?type=1",
    2: "/api/dashboard/diagnosis_page?type=2",
    3: "/api/dashboard/diagnosis_page?type=3",
}


@router.get("/api/dashboard/diagnosis_page", response_model=ApiEnvelope)
def diagnosis_page(type: int = Query(..., ge=1, le=3)):
    return get_interface_response(DATA_PATH, FIXED_KEY[type])
