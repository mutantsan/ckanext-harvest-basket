from __future__ import annotations

from typing import Any

import ckan.plugins.toolkit as tk


@tk.side_effect_free
def check_source(ctx: dict[str, Any], data_dict: dict) -> dict[str, Any]:
    tk.check_access("harvest_basket_check_source", ctx, data_dict)

    source_name: str = tk.get_or_bust(data_dict, "source_name")

    sources = ("dkan", "junar")

    if source_name not in sources:
        raise tk.ValidationError(f"The source checkup for type `{source_name}` not implemented")
    return {
        "result": source_name    
    }