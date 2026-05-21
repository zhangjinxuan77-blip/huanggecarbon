# -*- coding: utf-8 -*-
from fastapi import APIRouter
from pydantic import BaseModel

from modules.scope_summary_common import f2, summary_row


router = APIRouter()


class Scope2Body(BaseModel):
    timeType: int  # 1=日, 2=周, 3=月, 4=年


@router.post("/api/scope/scope_2")
def scope_2(body: Scope2Body):
    row = summary_row("scope2_电耗汇总", body.timeType, level="total")

    inside_carbon = f2(row.get("inside_plant_carbon_kg_sum"))
    outside_carbon = f2(row.get("outside_plant_carbon_kg_sum"))
    inside_kwh = f2(row.get("inside_plant_electricity_kwh_sum"))
    outside_kwh = f2(row.get("outside_plant_electricity_kwh_sum"))

    return {
        "code": 0,
        "msg": "",
        "data": {
            "totalCarbonEmissions": f2(inside_carbon + outside_carbon),
            "totalPlantElectricityConsumptionCarbonEmissions": inside_carbon,
            "totalElectricityConsumption": f2(inside_kwh + outside_kwh),
            "offSiteElectricityConsumptionCarbonEmissions": outside_carbon,
            "qsdElectricityConsumption": f2(row.get("intake_electricity_kwh_sum")),
            "qsdElectricityConsumptionCarbonEmissions": f2(row.get("intake_carbon_kg_sum")),
            "ssdElectricityConsumption": f2(row.get("supply_electricity_kwh_sum")),
            "ssdElectricityConsumptionCarbonEmissions": f2(row.get("supply_carbon_kg_sum")),
        },
    }
