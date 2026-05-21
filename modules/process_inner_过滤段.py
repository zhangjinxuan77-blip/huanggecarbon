# -*- coding: utf-8 -*-

from modules.process_stage_common import make_stage_router


router = make_stage_router(
    data_dir_name="05_过滤",
    route_base="/api/process/inner/过滤",
    info_fields=[
        {"field": "flipPlateSandFilterCE", "aliases": ["翻板砂滤池"]},
        {"field": "sandFilterBackwashPumpHouseCE", "aliases": ["砂滤反冲洗泵房"]},
        {"field": "carbonFilterBackwashPumpHouseCE", "aliases": ["炭滤反冲洗泵房"]},
    ],
    qtype_units={
        1: {"label": "翻板砂滤池", "share_label": "翻板砂滤池碳排", "aliases": ["翻板砂滤池"]},
        2: {"label": "砂滤反冲洗泵房", "share_label": "砂滤反冲洗泵房碳排", "aliases": ["砂滤反冲洗泵房"]},
        3: {"label": "炭滤反冲洗泵房", "share_label": "炭滤反冲洗泵房碳排", "aliases": ["炭滤反冲洗泵房"]},
    },
    share_label_key="工艺单元",
)
