# -*- coding: utf-8 -*-
"""
关键设备实时电耗碳排排行
Excel：data/设备Top10_日_仅排序.xlsx
sheet：默认第一个，列：A=设备名称，B=日碳排(kgCO2e)
"""

from fastapi import APIRouter, HTTPException
import pandas as pd
from typing import Optional, List, Dict, Any  # ★ 新增

router = APIRouter()

# 简单缓存，避免重复读 Excel（Python 3.9 用 Optional[List[Dict]]）
_TOP10_CACHE: Optional[List[Dict[str, Any]]] = None


def load_top10() -> List[Dict[str, Any]]:
    global _TOP10_CACHE
    if _TOP10_CACHE is not None:
        return _TOP10_CACHE

    path = "data/设备Top10_日_仅排序.xlsx"
    try:
        df = pd.read_excel(path, sheet_name=0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Excel 加载失败: {e}")

    # 兼容可能的列名写法
    cols = {str(c).strip(): c for c in df.columns}
    name_col = None
    value_col = None
    for key in cols:
        if "设备" in key and "名" in key:
            name_col = cols[key]
        if key in ("日", "日碳排", "日碳排量", "碳排日", "电耗_日"):
            value_col = cols[key]

    if name_col is None or value_col is None:
        raise HTTPException(
            status_code=500,
            detail="Excel 缺少“设备名称”或“日”数据列"
        )

    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        name = str(row[name_col]).strip()
        if not name or name == "nan":
            continue
        value = float(pd.to_numeric(row[value_col], errors="coerce") or 0.0)
        records.append({"name": name, "value": value})

    _TOP10_CACHE = records
    return records


@router.get("/api/process/device_top10")
def device_top10():
    """
    关键设备实时电耗碳排排行（Top10）
    无入参，直接返回 Excel 中的前 10 条记录。
    """
    data = load_top10()
    data = data[:10]      # 只取前 10

    return {
        "code": 0,
        "msg": "",
        "data": data,
    }
