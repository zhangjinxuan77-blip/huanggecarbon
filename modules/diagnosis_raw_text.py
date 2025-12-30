# -*- coding: utf-8 -*-
"""
碳排诊断TXT原样输出接口
接口：GET /api/dashboard/diagnosis
文件：data/碳排诊断输出.txt
"""

import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse

from modules.common import format_float_2d

router = APIRouter()

base_dir = os.path.dirname(os.path.dirname(__file__))
TXT_PATH = os.path.join(base_dir, "data", "碳排诊断输出.txt")

@router.get("/api/dashboard/diagnosis", response_class=PlainTextResponse)
def diagnosis_raw_text():
    if not os.path.exists(TXT_PATH):
        raise HTTPException(status_code=404, detail="未找到诊断文件")

    try:
        with open(TXT_PATH, "r", encoding="utf-8") as f:
            text = f.read()
    except Exception as e:
        raise HTTPException(status_code=500, detail="读取失败")

    # 你想原样显示，就直接 return，不用 JSON
    return PlainTextResponse(text, media_type="text/plain; charset=utf-8")
