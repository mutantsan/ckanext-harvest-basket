from __future__ import annotations
import json
import logging
from typing import Any


from ckan.plugins import toolkit as tk
from ckan.lib.munge import munge_name

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError, SearchError

from ckanext.harvest_basket.harvesters.base_harvester import BasketBasicHarvester

log = logging.getLogger(__name__)


class ArcGISHarvester(BasketBasicHarvester):
    SRC_ID = "ArcGIS"

    def info(self):
        return {
            "name": "arcgis",
            "title": "ArcGIS",
            "description": "Harvests datasets from remote ArcGIS portals",
        }

    def gather_stage(self, harvest_job):
        source_url = self._get_src_url(harvest_job)

        self._set_config(harvest_job.source.config)
        log.info(f"{self.SRC_ID}: gather stage started: {source_url}")

        try:
            pkg_dicts = self._search_datasets(source_url)
        except SearchError as e:
            log.error(f"{self.SRC_ID}: searching for datasets failed: {e}")
            self._save_gather_error(
                f"{self.SRC_ID}: unable to search the remote ArcGIS portal for datasets: {harvest_job}"
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
            package_ids = set()
            object_ids = []

            for pkg_dict in pkg_dicts:
                pkg_id: str = pkg_dict["id"]
                if pkg_id in package_ids:
                    log.debug(
                        f"{self.SRC_ID}: discarding duplicate dataset {pkg_id}. "
                        "Probably, due to datasets being changed in process of harvesting"
                    )
                    continue

                package_ids.add(pkg_id)

                log.info(
                    f"{self.SRC_ID}: creating harvest_object for package: {pkg_id}"
                )
                obj = HarvestObject(
                    guid=pkg_id, job=harvest_job, content=json.dumps(pkg_dict)
                )
                obj.save()
                object_ids.append(obj.id)

            return object_ids
        except Exception as e:
            log.debug(
                f"{self.SRC_ID}: the error occured during the gather stage: {e}"
            )
            self._save_gather_error("{}".format(e), harvest_job)
            return []

    def _search_datasets(self, source_url: str) -> list[dict[str, Any]]:
        services_dicts = []
        services_urls: list[str] = self._get_all_services_urls_list(source_url)
        max_datasets = tk.asint(self.config.get("max_datasets", 0))

        for service in services_urls:
            log.info(f"{self.SRC_ID}: gathering remote dataset: {service}")
            log.info(
                f"{self.SRC_ID}: progress "
                f"{services_urls.index(service) + 1}/{len(services_urls) + 1}"
            )
            service_meta = self._get_service_metadata(service)
            service_meta["resources"] = self._get_service_metadata(service, res=True)

            if not service_meta.get("id"):
                log.error(f"{self.SRC_ID}: the dataset has no id. Skipping...")
                continue

            services_dicts.append(service_meta)

            if max_datasets and len(services_dicts) == max_datasets:
                break

        return services_dicts

    def _get_all_services_urls_list(self, source_url: str) -> list[str]:
        """Fetches the list of service URLs

        Args:
            source_url (str): remote portal URL

        Raises:
            SearchError: raises an error if remote portal
                         response not a JSON

        Returns:
            list[str]: a list of URL strings
        """

        url = source_url + "/arcgis/rest/services?f=pjson"
        try:
            resp = self._make_request(url)
        except ContentFetchError as e:
            log.debug(
                f"{self.SRC_ID}: Could not fetch remote portal services list: {e}"
            )
            return []

        try:
            content = json.loads(resp.text)
        except ValueError as e:
            raise SearchError(
                f"{self.SRC_ID}: response from remote portal was not a JSON: {e}"
            )

        try:
            services_urls = [
                service["url"] for service in content["services"] if service.get("url")
            ]
        except KeyError as e:
            log.debug(
                f"{self.SRC_ID}: there is no available services in remote portal"
            )
            return []

        return services_urls

    def _get_service_metadata(self, service_url: str, res: bool = False) -> dict:
        """Fetches service metadata or service resource metadata
        Uses two different methods depends on `res` parameter

        Args:
            service_url (str): remote service URL
            res (bool, optional): Flag to change the fetch method. Defaults to False.

        Returns:
            dict: service metadata
        """
        param = "/?f=pjson" if res else "/info/itemInfo?f=pjson"

        try:
            resp = self._make_request((service_url + param))
        except ContentFetchError:
            log.debug(f"{self.SRC_ID}: Can't fetch the metadata. Access denied.")
            return []

        try:
            content = json.loads(resp.text)
        except ValueError as e:
            log.debug(
                f"{self.SRC_ID}: Can't fetch the metadata. JSON object is corrupted"
            )

        if res:
            resources = []
            for res in content["layers"] + content["tables"]:
                resources.append(res)

            return resources

        return content

    def _get_resource_url(self, pkg_id, res_id, fmt):
        offset = "https://opendata.arcgis.com/datasets/"
        return f"{offset}{pkg_id}_{res_id}.{fmt}"

    def fetch_stage(self, harvest_object):
        self.source_url = harvest_object.source.url.strip("/")
        package_dict = json.loads(harvest_object.content)
        self._pre_map_stage(package_dict, self.source_url)
        harvest_object.content = json.dumps(package_dict)
        return True

    def _pre_map_stage(self, package_dict, source_url):
        """Premap stage makes some basic changes to package data
        to prepare data for CKAN

        Args:
            package_dict (dict): remote package data
        """
        package_dict["notes"] = self._description_refine(
            package_dict.get("description", "")
        )
        package_dict["author"] = package_dict.get("owner", "")
        package_dict["name"] = munge_name(package_dict["name"])
        package_dict["title"] = self._refine_name(package_dict.get("title", ""))
        package_dict["tags"] = self._fetch_tags(package_dict.get("tags", []))
        package_dict["resources"] = self._resources_fetch(package_dict)
        package_dict["url"] = package_dict.get("url", "")
        package_dict["origin_type"] = package_dict.pop("type")
        package_dict["type"] = "dataset"

        package_dict["extras"] = []

        for field in ("accessInformation", "culture", "snippet"):
            if package_dict.get(field):
                package_dict["extras"].append(
                    {"key": field, "value": package_dict[field]}
                )

    def _refine_name(self, s: str) -> str:
        """Refines package titles to match CKAN requirements

        Args:
            s (str): name string

        Returns:
            str: refined name
        """
        while "_" in s or "  " in s:
            s = s.replace("_", " ").replace("  ", "")

        return s

    def _resources_fetch(self, pkg_data):
        resources = []

        # ArcGIS provides the same resources in different formats
        # We are gonna fetch the 3 most popular
        for fmt in ("csv", "geojson", "kml"):
            for res in pkg_data["resources"]:
                resource = {}
                resource_url = self._get_resource_url(pkg_data["id"], res["id"], fmt)
                resource["package_id"] = pkg_data["id"]
                resource["url"] = resource_url
                resource["format"] = fmt

                resource["name"] = res.get("name", "") + " ({})".format(fmt)
                resources.append(resource)

        return resources
