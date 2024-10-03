from __future__ import annotations

import json

from ckanext.dcat.harvesters import DCATJSONHarvester
from ckanext.transmute.utils import get_schema
from .base_harvester import BasketBasicHarvester


class BasketDcatJsonHarvester(DCATJSONHarvester, BasketBasicHarvester):
    SRC_ID = "DCAT"
    n = 0
    def info(self):
        return {
            "name": "basket_dcat_json",
            "title": "Extended DCAT JSON Harvester",
            "description": "Harvester with extended configuration "
            + "for DCAT dataset descriptions serialized as JSON",
        }

    def modify_package_dict(self, package_dict, dcat_dict, harvest_object):
        from icecream import ic
        ic(self.n)
        self.n += 1

        self.base_context = {"user": self._get_user_name()}

        package_dict = json.loads(harvest_object.content)
        self._set_config(harvest_object.source.config)

        schema = self.config.get("tsm_schema")
        if not schema and (schema_name := self.config.get("tsm_named_schema")):
            schema = get_schema(schema_name)

        self.transmute_data(package_dict, schema)

        return package_dict

    def _get_guids_and_datasets(self, content):
        for idx, item in enumerate(super()._get_guids_and_datasets(content)):
            yield item
