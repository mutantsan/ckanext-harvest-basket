import logging
import re
import json
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs4

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters.ckanharvester import SearchError

from ckanext.harvest_basket.harvesters.base_harvester import BasketBasicHarvester


log = logging.getLogger(__name__)


class JunarHarvester(BasketBasicHarvester):
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
        self.source_type = "Junar"
        self.source_url = harvest_job.source.url.strip("/")
        log.info(f"{self.source_type}: gather stage in progress: {self.source_url}")

        self._set_config(harvest_job.source.config)

        try:
            pkg_dicts = self._search_for_datasets(self.source_url)
        except SearchError as e:
            log.error(f"{self.source_type}: searching for datasets failed: {e}")
            self._save_gather_error(
                f"{self.source_type}: unable to search the remote portal for datasets: {self.source_url}",
                harvest_job,
            )
            return []

        if not pkg_dicts:
            log.error(f"{self.source_type}: searching returns empty result.")
            self._save_gather_error(
                f"{self.source_type}: no datasets found at remote portal: {self.source_url}",
                harvest_job,
            )
            return []

        try:
            object_ids = []

            for pkg_dict in pkg_dicts:
                log.info(
                    f"{self.source_type}: Creating HARVEST object "
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
                f"{self.source_type}: The error occured during the gather stage: {e}"
            )
            self._save_gather_error(str(e), harvest_job)
            return []

    def _search_for_datasets(self, source_url):
        pkg_dicts = []
        self.url = self._get_all_resources_data_url(source_url)

        max_datasets = int(self.config.get("max_datasets", 0))

        while True:
            log.info(f"{self.source_type}: Gathering remote dataset: {self.url}")

            resp = self._make_request(self.url)

            if resp:
                try:
                    pkgs_data = resp.json()
                except ValueError as e:
                    raise SearchError(
                        f"{self.source_type}: invalid response type, not a JSON"
                    )
            else:
                if "api/v2/resources" in self.url:
                    break
                log.debug(
                    f"{self.source_type}: Remote portal doesn't provide resources API. "
                    "Changing API endpoint"
                )
                self.url = self._get_all_datasets_json_url(source_url)
                continue

            package_ids = set()
            for pkg in pkgs_data["results"]:
                if "/dashboards/" in pkg["link"]:
                    # we can skip the dashboards, cause it's just a "collection"
                    # of resources that we will get anyway
                    break
                # Junar API returns duplicated IDs
                # They put the same resource in defferent "types"
                if pkg["guid"] in package_ids:
                    log.debug(
                        f"{self.source_type}: Discarding duplicate dataset {pkg['guid']}."
                    )
                    continue
                package_ids.add(pkg["guid"])

                pkg_dicts.append(pkg)

            url = pkgs_data.get("next")
            if not url:
                break

            if max_datasets and len(pkg_dicts) > max_datasets:
                break

        if max_datasets:
            return pkg_dicts[:max_datasets]
        return pkg_dicts

    def _get_all_datasets_json_url(self, source_url):
        auth_key = self._get_auth_key()

        return urljoin(
            source_url, f"/api/v2/datasets.json/?auth_key={auth_key}&limit=50"
        )

    def _get_all_resources_data_url(self, source_url):
        auth_key = self._get_auth_key()

        return urljoin(source_url, f"/api/v2/resources/?auth_key={auth_key}&limit=50")

    def _get_auth_key(self):
        auth_key = self.config.get("auth_key")

        if not auth_key:
            self.url = self.source_url
            raise SearchError(
                f"{self.source_type}: missing `auth_key`. "
                "Please, provide it via the config"
            )
        return auth_key

    def fetch_stage(self, harvest_object):
        package_dict = json.loads(harvest_object.content)
        self._pre_map_stage(package_dict)
        harvest_object.content = json.dumps(package_dict)
        return True

    def _pre_map_stage(self, package_dict: dict):
        package_dict["id"] = package_dict["guid"]
        package_dict["title"] = package_dict["title"]
        package_dict["notes"] = package_dict.get("description", "")
        package_dict["url"] = package_dict.get("link", "")
        package_dict["author"] = package_dict.get("user", "")
        package_dict["tags"] = self._fetch_tags(package_dict.get("tags", []))
        package_dict["created_at"] = self._datetime_refine(
            package_dict.get("created_at", "")
        )
        package_dict["type"] = "dataset"

        res_type = self._define_resource_type(package_dict["link"])

        package_dict["resources"] = self._fetch_resources(
            package_dict, res_type, self.source_url
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

    def _define_resource_type(self, resource_url):
        res_types = ("dataviews", "datasets", "visualizations")
        for res_type in res_types:
            if res_type in resource_url:
                return res_type

    def _fetch_resources(self, pkg_data, res_type, source_url):
        resources = []

        resource = {}

        resource["package_id"] = pkg_data["guid"]
        resource["url"] = self._get_resource_url(pkg_data, res_type, source_url)
        resource["format"] = "CSV"
        resource["created"] = self._datetime_refine(pkg_data.get("created_at", ""))
        resource["last_modified"] = self._datetime_refine(
            pkg_data.get("modified_at", "")
        )
        resource["name"] = pkg_data["title"]

        # if res_type is datasets, we can"t predict the format
        # otherwise we are fetching only csv
        if res_type == "datasets":
            fmt = "DATA"

            resp = self._make_request(resource["url"])
            if not resp:
                pass
            else:
                try:
                    fmt = resp.headers["Content-Disposition"].strip('"').split(".")[-1]
                except KeyError:
                    fmt = resp.url.split(".")[-1]
                    if "application/json" in reresps.headers.get("content-type", ""):
                        fmt = "JSON"

            resource["format"] = fmt.upper()

        resources.append(resource)

        return resources

    def _get_resource_url(self, pkg_data, res_type, source_url):
        auth_key = self._get_auth_key()

        base_url = re.findall(r"((http|https):\/\/(\w+\.*)+)", pkg_data["link"])[0][0]
        d = {
            "dataviews": self._get_dataview_resource_url,
            "datasets": self._get_dataset_resource_url,
            "visualizations": self._get_visualisation_resource_url,
        }

        return d[res_type](pkg_data, source_url, base_url) + "/?auth_key={}".format(
            auth_key
        )

    def _get_dataview_resource_url(self, pkg_data, source_url=None, base_url=None):
        return source_url + "/api/v2/datastreams/{}/data.csv".format(pkg_data["guid"])

    def _get_dataset_resource_url(self, pkg_data, source_url=None, base_url=None):
        url = pkg_data["link"].strip("/") + ".download"
        # 252295/ -> 252295-
        sub_str = re.search(r"(?P<digits>\d{1,10})(?P<slash>[\/])", url).group(0)
        return url.replace(sub_str, (sub_str.strip("/") + "-"))

    def _get_visualisation_resource_url(self, pkg_data, source_url=None, base_url=None):
        res = self._make_request(pkg_data["link"])

        if res:
            html = bs4(res.text)
            download_url = html.find(id="id_exportToCSVButton")
            if download_url:
                return base_url + download_url["href"]

        return ""
