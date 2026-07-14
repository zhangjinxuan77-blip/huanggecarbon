# -*- coding: utf-8 -*-
"""Helpers shared by the static dashboard interface modules."""

import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel, ValidationError


class ApiEnvelope(BaseModel):
    code: int
    msg: str
    data: dict[str, Any]


def load_interface_data(path: str) -> dict[str, Any]:
    data_path = Path(path)
    if not data_path.is_file():
        raise HTTPException(status_code=404, detail="未找到接口数据文件")

    try:
        with data_path.open(encoding="utf-8") as file:
            data = json.load(file)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise HTTPException(status_code=500, detail="接口数据文件格式错误") from exc
    except OSError as exc:
        raise HTTPException(status_code=500, detail="读取接口数据文件失败") from exc

    if not isinstance(data, dict):
        raise HTTPException(status_code=500, detail="接口数据文件顶层必须是 JSON 对象")
    return data


def get_interface_response(path: str, key: str) -> ApiEnvelope:
    data = load_interface_data(path)
    response = data.get(key)
    if response is None:
        raise HTTPException(status_code=404, detail=f"数据中不含 {key}")
    if not isinstance(response, dict):
        raise HTTPException(status_code=500, detail=f"接口数据 {key} 格式错误")
    try:
        return ApiEnvelope.model_validate(response)
    except ValidationError as exc:
        raise HTTPException(status_code=500, detail=f"接口数据 {key} 响应结构错误") from exc
