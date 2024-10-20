from __future__ import annotations

import json

from ckanext.dcat.harvesters import DCATJSONHarvester
from ckanext.transmute.utils import get_schema
from .base_harvester import BasketBasicHarvester


class BasketDcatJsonHarvester(DCATJSONHarvester, BasketBasicHarvester):
    SRC_ID = "DCAT"

    def info(self):
        return {
            "name": "basket_dcat_json",
            "title": "Extended DCAT JSON Harvester",
            "description": "Harvester with extended configuration "
            + "for DCAT dataset descriptions serialized as JSON",
        }

    def modify_package_dict(self, package_dict, dcat_dict, harvest_object):
        self.base_context = {"user": self._get_user_name()}

        package_dict = json.loads(harvest_object.content)
        self._set_config(harvest_object.source.config)

        self._transmute_content(package_dict)
        return package_dict
