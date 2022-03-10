import json
import logging
from urllib.parse import urljoin

import ckan.plugins.toolkit as tk
from ckan import model

from ckanext.harvest.harvesters import CKANHarvester
from ckanext.harvest.harvesters.ckanharvester import SearchError

from ckanext.harvest_basket.harvesters.base_harvester import BasketBasicHarvester


log = logging.getLogger(__name__)


class CustomCKANHarvester(CKANHarvester, BasketBasicHarvester):
    def import_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        self.transmute_data(package_dict)
        harvest_object.content = json.dumps(package_dict)

        super().import_stage(harvest_object)
    
    def _search_datasets(self, remote_url: str):
        url = remote_url.rstrip("/") + "/api/action/package_search?rows=1"
        resp = self._make_request(url)

        if not resp:
            return

        try:
            package_dict = json.loads(resp.text)["result"]["results"]
        except (ValueError, KeyError) as e:
            err_msg: str = f"CKAN: Response JSON doesn't contain result: {e}"
            log.error(err_msg)
            raise SearchError(err_msg)

        return package_dict
    
    def _pre_map_stage(self, data_dict):
        return data_dict

    def transmute_data(self, data):
        transmute_schema = self.config.get("tsm_schema")

        if transmute_schema:
            tk.get_action("tsm_transmute")(
                {
                    "model": model,
                    "session": model.Session,
                    "user": self._get_user_name()
                },
                {"data": data, "schema": transmute_schema}
            )
