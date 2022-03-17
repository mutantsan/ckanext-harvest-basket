import logging
import requests
import json
from urllib import parse
from urllib.parse import urljoin
from time import sleep

from ckan.lib.munge import munge_tag

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError, SearchError

from ckanext.harvest_basket.harvesters.base_harvester import BasketBasicHarvester


log = logging.getLogger(__name__)


class DKANHarvester(BasketBasicHarvester):
    PACKAGE_LIST: str = "/api/3/action/package_list"
    PACKAGE_SHOW: str = "/api/3/action/package_show"
    SRC_ID = "DKAN"

    def info(self):
        return {
            "name": "dkan",
            "title": "DKAN",
            "description": "Harvests remote DKAN instances",
        }

    def gather_stage(self, harvest_job):
        source_url = harvest_job.source.url.strip("/")
        log.info(f"{self.SRC_ID}: Starting gather_stage {source_url}")

        self._set_config(harvest_job.source.config)
        log.info(f"{self.SRC_ID}: Using config: {self.config}")

        try:
            pkg_dicts = self._search_datasets(source_url)
        except SearchError as e:
            log.error(f"{self.SRC_ID}: Searching for datasets failed: {e}")
            self._save_gather_error(
                f"{self.SRC_ID}: Unable to search remote portla for datasets: {source_url}",
                harvest_job,
            )
            return []

        if not pkg_dicts:
            self._save_gather_error(
                f"{self.SRC_ID}: No datasets found at remote portal: {source_url}",
                harvest_job,
            )
            return []

        package_ids = set()
        object_ids = []

        for pkg_dict in pkg_dicts:

            if pkg_dict["id"] in package_ids:
                log.debug(
                    f"{self.SRC_ID}: Discarding duplicate dataset {pkg_dict['id']}. "
                    "Probably, due to datasets being changed in process of harvesting"
                )
                continue

            package_ids.add(pkg_dict["id"])

            log.info(
                f"{self.SRC_ID}: Creating harvest_object for {pkg_dict.get('name', '')} {pkg_dict['id']}"
            )

            try:
                obj = HarvestObject(
                    guid=pkg_dict["id"], job=harvest_job, content=json.dumps(pkg_dict)
                )
                obj.save()
                object_ids.append(obj.id)
            except TypeError as e:
                log.debug(
                    f"{self.SRC_ID}: The error occured during the gather stage: {str(e)}"
                )
                self._save_gather_error(str(e), harvest_job)
                continue

        return object_ids

    def _search_datasets(self, remote_url):
        self.url = urljoin(remote_url, self.PACKAGE_LIST)
        pkg_dicts = []

        try:
            package_names = self._get_package_names(self.url)
        except ContentFetchError as e:
            raise SearchError(e)

        try:
            package_names = json.loads(package_names)["result"]
        except ValueError as e:
            raise SearchError(f"{self.SRC_ID}: response from remote portal was not a JSON: {e}")

        max_datasets = int(self.config.get("max_datasets", 0))
        delay = int(self.config.get("delay", 0))

        for package_name in set(package_names):
            url = f"{remote_url}{self.PACKAGE_SHOW}?{parse.urlencode({'id': package_name})}"
            log.debug(f"{self.SRC_ID}: Searching for dataset: {url}")

            resp = self._make_request(url)

            if not resp:
                continue

            try:
                package_dict_page = json.loads(resp.text)["result"]
            except ValueError as e:
                log.error(
                    f"{self.SRC_ID}: Response JSON doesn't contain result: {e}"
                )
                continue

            # some portals return a dict as result, not a list
            if "id" in package_dict_page:
                pkg_dict = []
                pkg_dict.append(package_dict_page)
                package_dict_page = pkg_dict

            pkg_dicts.extend(package_dict_page)

            if max_datasets and len(pkg_dicts) == max_datasets:
                break

            # to avoid ban for frequent requests
            # you can use delay parameter in config
            if delay > 0:
                sleep(delay)
                log.info(f"{self.SRC_ID}: Sleeping for {delay} second(s)")

        return pkg_dicts

    def _get_package_names(self, url):
        resp = requests.get(url)

        if resp.status_code == 200:
            return resp.text

        log.error("Bad response from remote portal")
        raise ContentFetchError(
            f"Request status_code: {resp.status_code}, reason: {resp.reason}"
        )

    def fetch_stage(self, harvest_object):
        self.source_url = harvest_object.source.url.strip("/")
        self._set_config(harvest_object.source.config)
        package_dict = json.loads(harvest_object.content)
        self._pre_map_stage(package_dict, self.source_url)
        harvest_object.content = json.dumps(package_dict)
        return True

    def _pre_map_stage(self, content: dict, source_url: str):
        content["resources"] = self._fetch_resources(
            content.get("resources"), content.get("id")
        )
        content["tags"] = self._fetch_tags(content.get("tags"))

        content["private"] = False
        content["state"] = content.get("state", "active").lower()
        content["type"] = "dataset"

        content["metadata_created"] = self._datetime_refine(
            content.get("metadata_created")
        )
        content["metadata_modified"] = self._datetime_refine(
            content.get("metadata_modified")
        )

        content["notes"] = self._description_refine(content.get("notes"))

        for key in ["groups", "log_message", "revision_timestamp", "creator_user_id"]:
            content.pop(key, None)

    def _fetch_tags(self, tags_list):
        tags = []
        if tags_list:
            for t in tags_list:
                tag = {}
                tag["name"] = munge_tag(t["name"])
                tag["id"] = t.get("id", "")
                tag["state"] = "active"
                tag["display_name"] = tag["name"]

                tags.append(tag)

        return tags

    def _fetch_resources(self, resource_list, pkg_id):
        resources = []
        if resource_list:
            for res in resource_list:
                resource = {}

                resource["description"] = self._description_refine(
                    res.get("description")
                )
                resource["id"] = res.get("id", "")
                resource["format"] = res.get("format", "").upper()

                resource["last_modified"] = self._datetime_refine(
                    res.get("last_modified")
                )
                resource["created"] = self._datetime_refine(res.get("created"))

                resource["mimetype"] = res.get("mimetype")

                resource["name"] = res.get("name", res.get("title", ""))
                resource["package_id"] = pkg_id
                resource["size"] = self._size_refine(res.get("size", None))
                resource["state"] = res.get("state", "active").lower()
                resource["url"] = res.get("url", "")

                resource.pop("revision_id", None)

                resources.append(resource)

        return resources

    def _size_refine(self, size_string):
        # if resource size stored as int
        try:
            if size_string and int(size_string):
                return int(size_string)
        except ValueError:
            pass

        if size_string:
            # bytes don't need convertion
            if "byte" in size_string.lower():
                return size_string.split(" ")[0]

            metric_sys = {"kb": 1e3, "mb": 1e6, "gb": 1e9}
            value, metric = size_string.lower().split(" ")
            return int(float(value) * metric_sys[metric])

        return 0
