# -*- coding: utf-8 -*-

from modules.process_stage_common import make_stage_router


router = make_stage_router(
    data_dir_name="01_原水取水段",
    route_base="/api/process/outer/原水取水",
    info_fields=[
        {"field": "rawWaterLiftingPumpHouseCE", "aliases": ["原水提升泵房"]},
        {"field": "waterIntakePumpingStationCE", "aliases": ["取水泵站"]},
        {"field": "sodiumHypochloriteRoomCE", "aliases": ["次氯酸钠间"]},
    ],
    qtype_units={
        1: {"label": "原水提升泵房", "share_label": "原水提升泵房", "aliases": ["原水提升泵房"]},
        2: {"label": "取水泵站", "share_label": "取水泵站", "aliases": ["取水泵站"]},
        3: {"label": "次氯酸钠间", "share_label": "次氯酸钠间", "aliases": ["次氯酸钠间"]},
    },
    share_label_key="工艺单元",
)
