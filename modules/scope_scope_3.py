# -*- coding: utf-8 -*-
from fastapi import APIRouter
from pydantic import BaseModel

from modules.scope_summary_common import f2, summary_row


router = APIRouter()


class TimeBody(BaseModel):
    timeType: int  # 1=日, 2=周, 3=月, 4=年


@router.post("/api/scope/scope_3")
def scope_3_total(body: TimeBody):
    row = summary_row("scope3_汇总", body.timeType, level="total")

    chem = f2(row.get("chemical_carbon_kg_sum"))
    sludge = f2(row.get("sludge_transport_carbon_kg_sum"))

    return {
        "code": 0,
        "msg": "",
        "data": {
            "totalCarbonEmissions": f2(row.get("scope3_total_carbon_kg_sum")),
            "chart": {
                "dimensions": ["name", "data"],
                "source": [
                    {"name": "药剂碳排", "data": chem},
                    {"name": "污泥运输", "data": sludge},
                ],
            },
        },
    }
