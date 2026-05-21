# -*- coding: utf-8 -*-

from modules.process_stage_common import make_stage_router


# 注意：腾讯文档里“水厂外-供水段”对应前端“厂内-清水处理”，
# 实时数据目录使用 02_供水段。
router = make_stage_router(
    data_dir_name="02_供水段",
    route_base="/api/process/inner/清水处理",
    info_fields=[
        {"field": "clearWaterTankCE", "aliases": ["送水泵房", "清水池"]},
        {"field": "ordinaryWaterRegulationCE", "aliases": ["普通水调节池"]},
    ],
    qtype_units={
        1: {"label": "清水池", "share_label": "清水池碳排", "aliases": ["送水泵房", "清水池"]},
        2: {"label": "普通水调节池", "share_label": "普通水调节池", "aliases": ["普通水调节池"]},
    },
    share_label_key="工艺单元",
)
