# -*- coding: utf-8 -*-
from fastapi import APIRouter
from pydantic import BaseModel

from modules.scope_summary_common import f2, summary_row, summary_rows


router = APIRouter()


class TimeBody(BaseModel):
    timeType: int  # 1=日, 2=周, 3=月, 4=年


CHEM_ORDER = ["O3", "次氯酸钠", "PAC", "PAM"]
CHEM_NAME_ALIASES = {
    "O3": "O3",
    "NaClO": "次氯酸钠",
    "PAC": "PAC",
    "PAM": "PAM",
}


@router.post("/api/scope/scope_3/chem")
def scope_3_chem(body: TimeBody):
    rows = summary_rows("scope3_药耗", body.timeType, level="detail")
    total_row = summary_row("scope3_药耗", body.timeType, level="total")

    dose_by_name = {name: 0.0 for name in CHEM_ORDER}
    carbon_by_name = {name: 0.0 for name in CHEM_ORDER}
    for _, row in rows.iterrows():
        name = CHEM_NAME_ALIASES.get(str(row.get("chemical_type", "")).strip())
        if not name:
            continue
        dose_by_name[name] = f2(row.get("total_chemical_kg_sum"))
        carbon_by_name[name] = f2(row.get("total_carbon_kg_sum"))

    emissions = [carbon_by_name[name] for name in CHEM_ORDER]
    doses = [dose_by_name[name] for name in CHEM_ORDER]

    return {
        "code": 0,
        "msg": "",
        "data": {
            "totalCarbonEmissionsChemicalAgents": f2(total_row.get("total_carbon_kg_sum")),
            "chart": {
                "xAxis": ["投加量", "碳排放量"],
                "yAxis": CHEM_ORDER,
                "data1": doses,
                "data2": emissions,
            },
        },
    }
