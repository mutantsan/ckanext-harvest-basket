import logging
import json
from urllib.parse import urljoin

from ckan.lib.munge import munge_name

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters.ckanharvester import SearchError

from ckanext.harvest_basket.harvesters.base_harvester import BasketBasicHarvester


log = logging.getLogger(__name__)


class JunarHarvester(BasketBasicHarvester):
    SRC_ID = "Junar"

    def info(self):
        return {
            "name": "junar",
            "title": "Junar",
            "description": (
                "Harvests datasets from Junar portals. "
                "Provide the `auth_key` before start. "
                "The key could be acquired at the target Junar portal"
            ),
        }

    def gather_stage(self, harvest_job):
        source_url = self._get_src_url(harvest_job)

        log.info(f"{self.SRC_ID}: gather stage in progress: {source_url}")

        self._set_config(harvest_job.source.config)

        try:
            pkg_dicts = self._search_datasets(source_url)
        except SearchError as e:
            log.error(f"{self.SRC_ID}: searching for datasets failed: {e}")
            self._save_gather_error(
                f"{self.SRC_ID}: unable to search the remote portal for datasets: {source_url}",
                harvest_job,
            )
            return []

        if not pkg_dicts:
            log.error(f"{self.SRC_ID}: searching returns empty result.")
            self._save_gather_error(
                f"{self.SRC_ID}: no datasets found at remote portal: {source_url}",
                harvest_job,
            )
            return []

        try:
            object_ids = []

            for pkg_dict in pkg_dicts:
                log.info(
                    f"{self.SRC_ID}: Creating HARVEST object "
                    f"for {pkg_dict['title']} | guid: {pkg_dict['guid']}"
                )

                obj = HarvestObject(
                    guid=pkg_dict["guid"], job=harvest_job, content=json.dumps(pkg_dict)
                )
                obj.save()
                object_ids.append(obj.id)

            return object_ids
        except Exception as e:
            log.debug(
                f"{self.SRC_ID}: The error occured during the gather stage: {e}"
            )
            self._save_gather_error(str(e), harvest_job)
            return []

    def _search_datasets(self, source_url):
        auth_key = self.config.get("auth_key")

        if not auth_key:
            raise SearchError(
                f"{self.SRC_ID}: missing `auth_key`. "
                "Please, provide it via the config"
            )

        pkg_dicts = []
        self.url = urljoin(source_url, f"/api/v2/datastreams/?auth_key={auth_key}&limit=100")
        offset = 0

        max_datasets = int(self.config.get("max_datasets", 0))

        while True:
            log.info(f"{self.SRC_ID}: gathering remote dataset: {self.url}")

            resp = self._make_request(f"{self.url}&offset={offset}")

            if resp:
                try:
                    pkgs_data = resp.json()
                except ValueError as e:
                    raise SearchError(
                        f"{self.SRC_ID}: invalid response type, not a JSON"
                    )
            else:
                raise SearchError(
                    f"{self.SRC_ID}: error accessing remote portal"
                )

            package_ids = set()

            package_list = pkgs_data.get("results") if isinstance(pkgs_data, dict) else pkgs_data
            if not package_list:
                break
            
            for pkg in package_list:
                if pkg["guid"] in package_ids:
                    log.debug(
                        f"{self.SRC_ID}: discarding duplicate dataset {pkg['guid']}."
                    )
                    continue
                package_ids.add(pkg["guid"])
                pkg_dicts.append(pkg)

            offset += 100
            if max_datasets and len(pkg_dicts) > max_datasets:
                break

        return pkg_dicts

    def fetch_stage(self, harvest_object):
        self._set_config(harvest_object.source.config)
        source_url = self._get_src_url(harvest_object)
        package_dict = json.loads(harvest_object.content)
        self._pre_map_stage(package_dict, source_url)
        harvest_object.content = json.dumps(package_dict)
        return True

    def _pre_map_stage(self, package_dict: dict, source_url: str):
        package_dict["id"] = package_dict["guid"]
        package_dict["notes"] = package_dict.get("description", "")
        package_dict["url"] = package_dict.get("link", "")
        package_dict["author"] = package_dict.get("user", "")
        package_dict["tags"] = self._fetch_tags(package_dict.get("tags", []))
        package_dict["created_at"] = self._datetime_refine(
            package_dict.get("created_at", "")
        )
        package_dict["type"] = "dataset"

        package_dict["name"] = munge_name(package_dict["title"])
        package_dict["resources"] = self._fetch_resources(
            package_dict, source_url
        )

        extra = (
            ("frequency", "Update Frequency"),
            ("category_name", "Category Name"),
            ("license", "License"),
            ("mbox", "Mailbox"),
            ("res_type", "Resource type"),
        )

        package_dict["extras"] = []

        for field in extra:
            if package_dict.get(field[0]):
                package_dict["extras"].append(
                    {"key": field[1], "value": package_dict[field[0]]}
                )

    def _fetch_resources(self, pkg_data, source_url):
        resource = {}

        resource["package_id"] = pkg_data["guid"]
        resource["url"] = self._get_resource_url(pkg_data, source_url)
        resource["format"] = "CSV"
        resource["created"] = self._datetime_refine(pkg_data.get("created_at", ""))
        resource["last_modified"] = self._datetime_refine(
            pkg_data.get("modified_at", "")
        )
        resource["name"] = pkg_data["title"]

        return [resource]

    def _get_resource_url(self, pkg_data, source_url):
        auth_key = self.config.get("auth_key")

        dataview_url: str = self._get_dataview_resource_url(pkg_data, source_url)
        return f"{dataview_url}/?auth_key={auth_key}"

    def _get_dataview_resource_url(self, pkg_data, source_url):
        return urljoin(source_url, f"/api/v2/datastreams/{pkg_data['guid']}/data.csv")
