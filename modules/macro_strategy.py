# -*- coding: utf-8 -*-
"""
宏观策略-厂网统筹接口
GET /api/dashboard/macro_strategy
数据源：data/南沙黄阁水厂_接口数据.json
"""

import os, json
from fastapi import APIRouter, HTTPException

router = APIRouter()

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "data", "南沙黄阁水厂_接口数据.json")


def _load() -> dict:
    if not os.path.exists(DATA_PATH):
        raise HTTPException(status_code=404, detail="未找到接口数据文件")
    with open(DATA_PATH, encoding="utf-8") as f:
        return json.load(f)


@router.get("/api/dashboard/macro_strategy")
def macro_strategy():
    data = _load()
    key  = "/api/dashboard/macro_strategy"
    if key not in data:
        raise HTTPException(status_code=404, detail="数据中不含 macro_strategy")
    return data[key]
