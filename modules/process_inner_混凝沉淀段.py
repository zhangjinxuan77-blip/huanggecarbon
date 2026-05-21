# -*- coding: utf-8 -*-

from modules.process_stage_common import make_stage_router


router = make_stage_router(
    data_dir_name="04_混凝沉淀",
    route_base="/api/process/inner/混凝沉淀",
    info_fields=[
        {
            "field": "foldablePlateReactionHorizontalFlowSedimentationTankCE",
            "aliases": ["折板反应平流沉淀池"],
        },
    ],
    qtype_units={
        1: {
            "label": "折板反应平流沉淀池",
            "share_label": "折板反应平流沉淀池",
            "aliases": ["折板反应平流沉淀池"],
        },
    },
    share_label_key="碳排结构",
)
