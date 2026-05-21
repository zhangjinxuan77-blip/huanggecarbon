# -*- coding: utf-8 -*-
import os, re, pandas as pd
from typing import Any, Tuple

APP_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(APP_DIR, "data")

TIME_MAP = {1: "日", 2: "周", 3: "月", 4: "年"}

TIME_ALIASES = ["周期", "Period", "period", "频率", "时间", "timeType", "TimeType"]
S1_ALIASES   = ["范围1", "Scope1", "scope1", "S1", "scope 1"]
S2_ALIASES   = ["范围2", "Scope2", "scope2", "S2", "scope 2"]
S3_ALIASES   = ["范围3", "Scope3", "scope3", "S3", "scope 3"]

def norm(s: str) -> str:
    s = str(s)
    s = s.replace("：", ":").replace("（", "(").replace("）", ")")
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"\s+", "", s)
    return s.strip()

def auto_header(df0: pd.DataFrame, scan_rows: int = 50) -> pd.DataFrame:
    probe = df0.head(scan_rows).fillna("").astype(str)
    for ridx in probe.index:
        vals = [norm(v) for v in probe.loc[ridx].tolist()]
        joined = "|".join(vals)
        if any(norm(k) in joined for k in TIME_ALIASES) and ("范围" in joined or "Scope" in joined or "scope" in joined):
            df = df0.copy()
            df.columns = vals
            df = df.loc[ridx+1:].reset_index(drop=True)
            return df
    df = df0.copy()
    df.columns = [norm(x) for x in df.columns]
    return df

def pick(cols, aliases):
    cols_n = [norm(c) for c in cols]
    for a in aliases:
        a = norm(a)
        for c in cols_n:
            if a in c:
                return c
    return None

def standardize_period(x: Any) -> str:
    x = str(x).strip()
    mapping = {
        "1":"日","日":"日","day":"日","Day":"日",
        "2":"周","周":"周","week":"周","Week":"周",
        "3":"月","月":"月","month":"月","Month":"月",
        "4":"年","年":"年","year":"年","Year":"年",
    }
    return mapping.get(x, x)

def load_table_from_excel(filename: str, sheet_index: int = 0) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"未找到Excel：{path}")
    raw = pd.read_excel(path, sheet_name=sheet_index, header=None)
    return auto_header(raw)

def pick_scopes(df: pd.DataFrame) -> Tuple[str, str, str, str]:
    cols = list(df.columns)
    pc  = pick(cols, TIME_ALIASES)
    s1c = pick(cols, S1_ALIASES + ["范围1"])
    s2c = pick(cols, S2_ALIASES + ["范围2"])
    s3c = pick(cols, S3_ALIASES + ["范围3"])
    if not (pc and s1c and s2c and s3c):
        raise ValueError(f"缺少必要列，实际列：{cols}")
    return pc, s1c, s2c, s3c

from decimal import Decimal
import numpy as np

def format_float_2d(x):
    """
    递归：把所有 float / numpy.float / Decimal 统一 round 到 2 位
    只用于 API 输出层（不动计算层精度）
    """
    if isinstance(x, dict):
        return {k: format_float_2d(v) for k, v in x.items()}
    if isinstance(x, list):
        return [format_float_2d(v) for v in x]
    if isinstance(x, tuple):
        return tuple(format_float_2d(v) for v in x)
    if isinstance(x, (float, np.floating, Decimal)):
        return round(float(x), 2)
    return x
