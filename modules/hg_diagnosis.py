# -*- coding: utf-8 -*-
"""
优化策略诊断接口
GET /api/dashboard/diagnosis_page?type=1|2|3
数据源：data/南沙黄阁水厂_接口数据.json
"""

import os, json
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "南沙黄阁水厂_接口数据.json")
FIXED_KEY = {
    1: "/api/dashboard/diagnosis_page?type=1",
    2: "/api/dashboard/diagnosis_page?type=2",
    3: "/api/dashboard/diagnosis_page?type=3",
}


def _load() -> dict:
    if not os.path.exists(DATA_PATH):
        raise HTTPException(status_code=404, detail="未找到接口数据文件")
    with open(DATA_PATH, encoding="utf-8") as f:
        return json.load(f)


@router.get("/api/dashboard/diagnosis_page", operation_id="hg_diagnosis_page")
def hg_diagnosis_page(type: int = Query(..., ge=1, le=3)):
    data = _load()
    key  = FIXED_KEY[type]
    if key not in data:
        raise HTTPException(status_code=404, detail=f"数据中不含 {key}")
    return data[key]
