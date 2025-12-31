# -*- coding: utf-8 -*-
"""
诊断“页面化展示”接口（返回HTML）
接口：GET /api/dashboard/diagnosis_page
文件：data/碳排诊断输出.txt
"""

import os, re
from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

router = APIRouter()

base_dir = os.path.dirname(os.path.dirname(__file__))
TXT_PATH = os.path.join(base_dir, "data", "碳排诊断输出.txt")


def _read_txt() -> str:
    if not os.path.exists(TXT_PATH):
        raise HTTPException(status_code=404, detail="未找到诊断文件")
    try:
        with open(TXT_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        raise HTTPException(status_code=500, detail="读取失败")


def _pick_line(prefix: str, text: str, default: str = "—") -> str:
    """
    取以 prefix 开头的整行（包含单位），例如：
    prefix="标准限值:"  -> "标准限值: 0.4 mg/L"
    """
    m = re.search(rf"^{re.escape(prefix)}\s*(.+)$", text, flags=re.MULTILINE)
    return f"{prefix} {m.group(1).strip()}" if m else default


def _has(text: str, s: str) -> bool:
    return s in text


def _build_html(text: str) -> str:
    # —— 范围1（O3）——（整行抓取，保留单位）
    o3_limit_line  = _pick_line("标准限值:", text)
    o3_actual_line = _pick_line("计算O3投加浓度:", text)
    o3_ok = "未超标" if _has(text, "✅ O3投加浓度未超标") else "超标"
    o3_over_line = "超标量: 0" if o3_ok == "未超标" else _pick_line("超标量:", text, default="超标量: —")

    # —— 范围2（能耗热点）——（如果 txt 没有“能耗热点”行，就回退成默认文案）
    energy_hot_line = "能耗热点：无"
    if _has(text, "❌ 能耗热点: 取水泵站"):
        energy_hot_line = "能耗热点：取水泵站"

    # 你也可以直接抓 txt 里的“可能原因/优化策略”整段，但你目前 txt 是多行列表，不太好一行抓
    # 所以这里先保留你原来的固定描述（不涉及单位）
    energy_reason = "水泵叶轮磨损或汽蚀，运行工况点偏离高效区"
    energy_strategy = "设备离线检修，优化泵组组合"

    # —— 范围3（PAC）——（抓“当前PAC投加量/正常范围/超出上限”等行，保留单位）
    pac_current_line = _pick_line("当前PAC投加量:", text, default="当前PAC投加量: —")
    pac_range_line   = _pick_line("正常范围:", text, default="正常范围: —")

    pac_hot = "PAC投加" if _has(text, "❌ PAC投加量超标") else "无"
    pac_reason = "原水水质波动，投加量增大，药剂投加效率低下"
    pac_strategy = "自适应投加控制，优化混凝水力条件"

    # 诊断时间整行（保留原格式）
    ts_line = _pick_line("诊断时间:", text, default="")

    html = f"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>优化策略</title>
  <style>
    body {{
      margin: 0;
      background: #061426;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", Arial, sans-serif;
      color: #e9f2ff;
    }}
    .wrap {{ max-width: 980px; margin: 24px auto; padding: 16px; }}
    .panel {{
      border: 2px solid rgba(120, 190, 255, 0.35);
      border-radius: 16px;
      padding: 18px 18px 8px;
      background: radial-gradient(1200px 600px at 10% 10%, rgba(30,120,255,0.18), rgba(0,0,0,0.35)),
                  linear-gradient(180deg, rgba(14,55,120,0.55), rgba(3,12,24,0.65));
      box-shadow: 0 10px 30px rgba(0,0,0,0.35);
      position: relative;
      overflow: hidden;
    }}
    .header {{ display:flex; align-items:center; gap:12px; margin-bottom: 14px; }}
    .header .title {{ font-size: 26px; font-weight: 800; color: #ffcc66; letter-spacing: 1px; }}
    .header .sub {{ font-size: 12px; opacity: 0.75; }}
    .section {{ border-top: 1px solid rgba(120,190,255,0.25); padding: 14px 6px 18px; }}
    .sec-title {{ font-size: 22px; font-weight: 800; color: #ffcc66; margin: 0 0 10px 0; }}
    .row {{ display: grid; grid-template-columns: 220px 1fr; gap: 10px 16px; padding: 6px 0; }}
    .k {{ opacity: 0.9; font-size: 16px; letter-spacing: 1px; }}
    .v {{ font-size: 18px; font-weight: 700; white-space: pre-wrap; }}
    .ok {{ color: #8dffb2; }}
    .bad {{ color: #ff7b7b; }}
    .hint {{ opacity: 0.75; font-size: 13px; margin-top: 8px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="panel">
      <div class="header">
        <div class="title">优化策略</div>
        <div class="sub">{ts_line}</div>
      </div>

      <div class="section">
        <div class="sec-title">范围1</div>
        <div class="row"><div class="k">O3设备间O3浓度标准：</div><div class="v">{o3_limit_line}</div></div>
        <div class="row"><div class="k">O3实际浓度：</div><div class="v">{o3_actual_line}</div></div>
        <div class="row"><div class="k">超标量：</div><div class="v {"ok" if o3_ok=="未超标" else "bad"}">{o3_over_line}（{o3_ok}）</div></div>
        <div class="row"><div class="k">优化策略：</div><div class="v">各项指标一切正常</div></div>
      </div>

      <div class="section">
        <div class="sec-title">范围2</div>
        <div class="row"><div class="k">能耗热点：</div><div class="v {"bad" if "取水泵站" in energy_hot_line else "ok"}">{energy_hot_line}</div></div>
        <div class="row"><div class="k">可能原因：</div><div class="v">{energy_reason}</div></div>
        <div class="row"><div class="k">优化策略：</div><div class="v">{energy_strategy}</div></div>
      </div>

      <div class="section">
        <div class="sec-title">范围3</div>
        <div class="row"><div class="k">高碳排热点：</div><div class="v {"bad" if pac_hot!="无" else "ok"}">{pac_hot}</div></div>
        <div class="row"><div class="k">PAC投加：</div><div class="v">{pac_current_line}\n{pac_range_line}</div></div>
        <div class="row"><div class="k">可能原因：</div><div class="v">{pac_reason}</div></div>
        <div class="row"><div class="k">优化策略：</div><div class="v">{pac_strategy}</div></div>
      </div>

      <div class="hint">说明：本页面数据来自 data/碳排诊断输出.txt，展示内容保留 txt 原单位与原描述。</div>
    </div>
  </div>
</body>
</html>
"""
    return html


@router.get("/api/dashboard/diagnosis_page", response_class=HTMLResponse)
def diagnosis_page():
    text = _read_txt()
    return HTMLResponse(_build_html(text))
