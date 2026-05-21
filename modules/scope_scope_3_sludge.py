# -*- coding: utf-8 -*-
from fastapi import APIRouter
from pydantic import BaseModel

from modules.scope_summary_common import f2, summary_row


router = APIRouter()


class TimeBody(BaseModel):
    timeType: int  # 1=日, 2=周, 3=月, 4=年


@router.post("/api/scope/scope_3/sludge")
def scope_3_sludge(body: TimeBody):
    row = summary_row("scope3_汇总", body.timeType, level="total")

    sludge_ton = f2(row.get("sludge_transport_t_sum"))
    solid_rate_pct = f2(float(row.get("sludge_solid_rate_avg") or 0) * 100)
    distance_m = f2(float(row.get("sludge_transport_distance_km_avg") or 0) * 1000)

    return {
        "code": 0,
        "msg": "",
        "data": {
            "carbonEmissionsSludgeTransportation": f2(row.get("sludge_transport_carbon_kg_sum")),
            "chart": [
                ["<div style='color:#F9DA68'>污泥量</div>", f"{sludge_ton:.2f} 吨"],
                ["<div style='color:#F9DA68'>含固率</div>", f"{solid_rate_pct:.2f}%"],
                ["<div style='color:#F9DA68'>运输距离</div>", f"{distance_m:.2f} 米"],
            ],
        },
    }
