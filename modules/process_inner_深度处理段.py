# -*- coding: utf-8 -*-

from modules.process_stage_common import make_stage_router


router = make_stage_router(
    data_dir_name="06_深度处理",
    route_base="/api/process/inner/深度处理",
    info_fields=[
        {"field": "ozoneWorkshopCE", "aliases": ["臭氧车间"]},
        {"field": "mainOzoneContactTankCE", "aliases": ["主臭氧接触池"]},
        {"field": "flipPlateCarbonFilterCE", "aliases": ["翻板炭滤池"]},
        {"field": "sodiumHypochloriteDosingRoomCE", "aliases": ["次氯酸钠投加间"]},
    ],
    qtype_units={
        1: {"label": "臭氧车间", "share_label": "臭氧车间碳排", "aliases": ["臭氧车间"]},
        2: {"label": "主臭氧接触池", "share_label": "主臭氧接触池碳排", "aliases": ["主臭氧接触池"]},
        3: {"label": "翻板炭滤池", "share_label": "翻板炭滤池碳排", "aliases": ["翻板炭滤池"]},
        4: {"label": "次氯酸钠投加间", "share_label": "次氯酸钠投加间碳排", "aliases": ["次氯酸钠投加间"]},
    },
    share_label_key="工艺单元",
)
