# -*- coding: utf-8 -*-
from fastapi import APIRouter
from pydantic import BaseModel

from modules.scope_summary_common import f2, summary_row


router = APIRouter()


class TimeBody(BaseModel):
    timeType: int  # 1=日, 2=周, 3=月, 4=年


@router.post("/api/scope/scope_1")
def scope_1(body: TimeBody):
    row = summary_row(
        "scope1_carbon_outputs/21_范围一_臭氧泄漏率和臭氧实际产量",
        body.timeType,
        level="total",
    )

    return {
        "code": 0,
        "msg": "",
        "data": {
            "leakageCarbonEmissions": f2(row.get("unit_total_carbon_kg_sum")),
            "dosage": f2(row.get("A_actual_ozone_kg_sum")),
            "leakageAmount": f2(row.get("unit_total_chemical_kg_sum")),
        },
    }
