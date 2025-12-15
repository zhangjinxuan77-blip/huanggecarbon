# -*- coding: utf-8 -*-
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# ===== Dashboard =====
from modules.dashboard_overview import router as dashboard_overview_router
from modules.dashboard_scope_summary import router as dashboard_scope_summary_router
from modules.dashboard_scope_unit_intensity import router as dashboard_scope_unit_intensity_router
from modules.dashboard_scope_share import router as dashboard_scope_share_router

# ===== Section =====
from modules.process_section_total import router as process_section_total_router
from modules.process_section_share import router as process_section_share_router

# ===== Scope =====
from modules.scope_scope_1 import router as scope_scope_1_router
from modules.scope_scope_2 import router as scope_scope_2_router
from modules.scope_scope_3 import router as scope_scope_3_router
from modules.scope_scope_3_chem import router as scope_scope_3_chem_router
from modules.scope_scope_3_sludge import router as scope_scope_3_sludge_router

# ===== Inner processes =====
from modules.process_inner_预处理 import router as process_inner_预处理_router
from modules.process_inner_混凝沉淀段 import router as process_inner_混凝沉淀段_router
from modules.process_inner_深度处理段 import router as process_inner_深度处理段_router
from modules.process_inner_过滤段 import router as process_inner_过滤段_router
from modules.process_inner_污泥处理段 import router as process_inner_污泥处理段_router
from modules.process_inner_清水处理段 import router as process_inner_清水处理段_router

# ===== Outer processes =====
from modules.process_outer_原水取水段 import router as process_outer_原水取水段_router


app = FastAPI(title="Carbon API", version="1.0.0")

# ===== CORS =====
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== Register routers =====
app.include_router(dashboard_overview_router)
app.include_router(dashboard_scope_summary_router)
app.include_router(dashboard_scope_unit_intensity_router)
app.include_router(dashboard_scope_share_router)

app.include_router(process_section_total_router)
app.include_router(process_section_share_router)

app.include_router(scope_scope_1_router)
app.include_router(scope_scope_2_router)
app.include_router(scope_scope_3_router)
app.include_router(scope_scope_3_chem_router)
app.include_router(scope_scope_3_sludge_router)

app.include_router(process_inner_预处理_router)
app.include_router(process_inner_混凝沉淀段_router)
app.include_router(process_inner_深度处理段_router)
app.include_router(process_inner_过滤段_router)
app.include_router(process_inner_污泥处理段_router)
app.include_router(process_inner_清水处理段_router)

app.include_router(process_outer_原水取水段_router)


@app.get("/api/_health")
def health():
    return {"code": 0, "msg": "ok"}
