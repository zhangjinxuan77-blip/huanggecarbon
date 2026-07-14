from pydantic import BaseModel, ConfigDict, Field
from typing import Optional
from datetime import datetime


class FiniteModel(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)


class PlantData(FiniteModel):
    plant_id: str
    timestamp: datetime
    period: str = "日报"
    total_energy_consumption: float = Field(..., ge=0, description="kWh")
    carbon_emission_energy: float = Field(..., ge=0, description="kgCO₂e 范围2电耗碳排")
    carbon_emission_chemical: float = Field(..., ge=0, description="kgCO₂e 范围3药耗碳排")
    carbon_emission_o3_leakage: Optional[float] = Field(
        None, ge=0, description="kgCO₂e 范围1 O3泄漏碳排；None时由引擎从DisinfectionData字段计算"
    )
    carbon_emission_sludge_transport: Optional[float] = Field(
        None, ge=0, description="kgCO₂e 污泥运输碳排；用于跨周期总碳排和结构同口径比较"
    )
    water_volume_m3: Optional[float] = Field(
        None, ge=0, description="m³/day 日供水量 HGS_CQCLSN_TJB10_In_water_flow 积分；None时用config缺省值"
    )


class CoagulationData(FiniteModel):
    pac_consumption_kg: Optional[float] = Field(None, ge=0, description="kg/day PAC实际用量 TJL_PAC")
    pam_consumption_kg: Optional[float] = Field(None, ge=0, description="kg/day PAM实际用量 TJL_PAM")


class FiltrationData(FiniteModel):
    pass  # 反冲洗泵运行状态待接入SCADA，无水质/水量测量字段


class DisinfectionData(FiniteModel):
    sodium_hypochlorite_consumption_kg: Optional[float] = Field(
        None, ge=0, description="kg/day 次氯酸钠 TJL_NaClO"
    )
    ozone_consumption_kg: Optional[float] = Field(
        None, ge=0, description="kg/day O3实际产量 SCL_O3，字段HGS_CY_OZONE_ACTUAL_In_value"
    )
    ozone_leakage_rate: Optional[float] = Field(
        None, ge=0, le=1, description="O3泄漏率 XLL_O3 0–1，字段HGS_2_CYXT_AI9等"
    )


class PumpStationData(FiniteModel):
    station_id: str
    energy_consumption: float = Field(..., ge=0, description="kWh，字段如HGS_SSBF_P1_In_activeEnergy")


class PipelineSegmentData(FiniteModel):
    segment_id: str
    pressure_drop: float = Field(..., description="MPa")
    flow_rate: float = Field(..., description="m³/h")
    energy_consumption: float = Field(..., ge=0, description="kWh")


class SludgeDewatering(FiniteModel):
    pam_consumption_kg: Optional[float] = Field(None, ge=0, description="kg/day 脱水PAM实际用量")
    sludge_weight_tons: Optional[float] = Field(None, ge=0, description="吨/day 污泥量 WNL_M")
    sludge_solid_rate: Optional[float] = Field(None, ge=0, le=1, description="含固率 0–1 WNHGL_S")
    sludge_transport_km: Optional[float] = Field(None, ge=0, description="km 运输距离 YSJL_D")


class ProcessUnits(FiniteModel):
    coagulation_sedimentation: CoagulationData
    filtration: FiltrationData
    disinfection: DisinfectionData
    pump_stations: list[PumpStationData]
    pipeline_segments: list[PipelineSegmentData]
    sludge_dewatering: Optional[SludgeDewatering] = None


class ReportRequest(FiniteModel):
    plant: PlantData
    units: ProcessUnits
    previous_plant: Optional[PlantData] = Field(
        None, description="昨日全厂数据，M1碳排结构波动/M2碳排强度日环比所需"
    )


class Layer2Report(BaseModel):
    coagulation: str
    filtration: Optional[str] = None
    disinfection: str
    pump_stations: str
    pipeline_segments: Optional[str] = None
    sludge_dewatering: Optional[str] = None


class ReportResponse(BaseModel):
    meta: dict
    layer1_text: str
    layer2: Layer2Report
    layer3_insights: list[str]
    full_report_text: str
    flags: dict


# ── Dashboard 数据模型（供 GoView Pro 绑定）──────────────────────────────────────

class CarbonScopeItem(BaseModel):
    name: str
    scope: int
    value: float

class ProcessUnitCarbon(BaseModel):
    name: str
    carbon: float
    type: str
    pct: float

class DashboardAlert(BaseModel):
    rule: str
    level: str
    summary: str
    detail: str

class DashboardResponse(BaseModel):
    meta: dict
    kpi: dict
    carbon_by_scope: list[CarbonScopeItem]
    carbon_by_process: list[ProcessUnitCarbon]
    pump_stations: list[dict]
    pipeline_segments: list[dict]
    alerts: list[DashboardAlert]
    report_text: str
