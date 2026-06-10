# -*- coding: utf-8 -*-
"""
工艺策略 / 逻辑策略接口
GET /api/dashboard/lowcarbon/strategies
GET /api/dashboard/lowcarbon/evaluation
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


@router.get("/api/dashboard/lowcarbon/strategies")
def lowcarbon_strategies():
    data = _load()
    key  = "/api/dashboard/lowcarbon/strategies"
    if key not in data:
        raise HTTPException(status_code=404, detail="数据中不含 strategies")
    return data[key]


@router.get("/api/dashboard/lowcarbon/evaluation")
def lowcarbon_evaluation():
    data = _load()
    key  = "/api/dashboard/lowcarbon/evaluation"
    if key not in data:
        raise HTTPException(status_code=404, detail="数据中不含 evaluation")
    return data[key]
