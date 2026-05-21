# -*- coding: utf-8 -*-

from modules.process_stage_common import make_stage_router


router = make_stage_router(
    data_dir_name="07_污泥处理",
    route_base="/api/process/inner/污泥处理",
    info_fields=[
        {"field": "recycledWaterTankCE", "aliases": ["回收水池"]},
        {"field": "sludgeConditioningTankCE", "aliases": ["污泥调节池"]},
        {"field": "sludgeThickeningTankCE", "aliases": ["污泥浓缩池"]},
        {"field": "sludgePumpHouseCE", "aliases": ["污泥泵房"]},
        {"field": "pamRoomDewateringRoomCE", "aliases": ["PAM", "脱水间"]},
    ],
    qtype_units={
        1: {"label": "回收水池", "share_label": "回收水池碳排", "aliases": ["回收水池"]},
        2: {"label": "污泥调节池", "share_label": "污泥调节池碳排", "aliases": ["污泥调节池"]},
        3: {"label": "污泥浓缩池", "share_label": "污泥浓缩池碳排", "aliases": ["污泥浓缩池"]},
        4: {"label": "污泥泵房", "share_label": "污泥泵房碳排", "aliases": ["污泥泵房"]},
        5: {
            "label": "污泥PAM投加间、脱水间",
            "share_label": "污泥PAM投加间、脱水间碳排",
            "aliases": ["PAM", "脱水间"],
        },
    },
    share_label_key="工艺单元",
)
