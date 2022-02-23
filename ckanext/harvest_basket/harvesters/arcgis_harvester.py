from __future__ import annotations
import json
import logging
from urllib import parse


from ckan import model
from ckan.plugins import toolkit as tk

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError, SearchError

from ckanext.harvest_basket.harvesters.base_harvester import BasketBasicHarvester

log = logging.getLogger(__name__)


class ArcGISHarvester(BasketBasicHarvester):
    def info(self):
        return {
            "name": "arcgis",
            "title": "ArcGIS",
            "description": "Harvests datasets from remote ArcGIS portals",
        }

    def gather_stage(self, harvest_job):
        self.source_type = "ArcGIS"
        self.source_url = harvest_job.source.url.strip("/")

        self._set_config(harvest_job.source.config)
        log.info(f"{self.source_type}: gather stage started: {self.source_url}")

        try:
            pkg_dicts = self._search_for_datasets(self.source_url)
        except SearchError as e:
            log.error(f"{self.source_type}: searching for datasets failed: {e}")
            self._save_gather_error(
                f"{self.source_type}: unable to search the remote ArcGIS portal for datasets: {harvest_job}"
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
            package_ids = set()
            object_ids = []

            for pkg_dict in pkg_dicts:
                pkg_id: str = pkg_dict["id"]
                if pkg_id in package_ids:
                    log.debug(
                        f"{self.source_type}: discarding duplicate dataset {pkg_id}. "
                        "Probably, due to datasets being changed in process of harvesting"
                    )
                    continue

                package_ids.add(pkg_id)

                log.info(
                    f"{self.source_type}: creating harvest_object for package: {pkg_id}"
                )
                obj = HarvestObject(
                    guid=pkg_id, job=harvest_job, content=json.dumps(pkg_dict)
                )
                obj.save()
                object_ids.append(obj.id)

            return object_ids
        except Exception as e:
            log.debug(
                f"{self.source_type}: the error occured during the gather stage: {e}"
            )
            self._save_gather_error("{}".format(e), harvest_job)
            return []

    def _search_for_datasets(self, source_url):
        services_dicts = []
        services_urls: list[str] = self._get_all_services_urls_list(source_url)
        max_datasets = tk.asint(self.config.get("max_datasets", 1))

        for service in services_urls:
            log.info(f"{self.source_type}: gathering remote dataset: {service}")
            log.info(
                f"{self.source_type}: progress "
                f"{services_urls.index(service) + 1}/{len(services_urls) + 1}"
            )
            service_meta = self._get_service_metadata(service)
            service_meta["resources"] = self._get_service_metadata(service, res=True)

            if not service_meta.get("id"):
                log.error(f"{self.source_type}: the dataset has no id. Skipping...")
                continue

            services_dicts.append(service_meta)

            if max_datasets and len(services_dicts) == max_datasets:
                break

        return services_dicts

    def _get_all_services_urls_list(self, source_url):
        url = source_url + "/arcgis/rest/services?f=pjson"
        try:
            resp = self._make_request(url)
        except ContentFetchError as e:
            log.debug(
                f"{self.source_type}: Could not fetch remote portal services list: {e}"
            )
            return []

        try:
            content = json.loads(resp.text)
        except ValueError as e:
            raise SearchError(f"{self.source_type}: response from remote portal was not a JSON: {e}")

        try:
            services_urls = [
                service["url"] for service in content["services"] if service.get("url")
            ]
        except KeyError as e:
            log.debug(
                f"{self.source_type}: there is no available services in remote portal"
            )
            return []

        return services_urls

    def _get_service_metadata(self, service_url, layers=False, res=False):
        """
        fetch service metadata or service resource metadata
        depends on res True/False parameter
        returns dict
        """
        param = "/info/itemInfo?f=pjson"
        if res:
            param = "/?f=pjson"

        try:
            resp = self._make_request((service_url + param))
        except ContentFetchError:
            log.debug(
                f"{self.source_type}: Can't fetch the metadata. Access denied."
            )
            return []
        
        try:
            content = json.loads(resp.text)
        except ValueError as e:
            log.debug(
                f"{self.source_type}: Can't fetch the metadata. JSON object is corrupted"
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
        package_dict = json.loads(harvest_object.content)
        self._pre_map_stage(package_dict)
        harvest_object.content = json.dumps(package_dict)
        return True

    def _pre_map_stage(self, package_dict):
        package_dict["id"] = package_dict["id"]
        package_dict["notes"] = self._description_refine(
            package_dict.get("description", "")
        )
        package_dict["author"] = package_dict.get("owner", "")
        package_dict["title"] = self._refine_name(package_dict.get("title", ""))
        package_dict["tags"] = self._fetch_tags(package_dict.get("tags", []))
        package_dict["resources"] = self._resources_fetch(package_dict)
        package_dict["url"] = package_dict.get("url", "")

        # fetching extras from remote portal
        package_dict["extras"] = []

        for field in ("accessInformation", "culture", "snippet"):
            if package_dict.get(field):
                package_dict["extras"].append(
                    {"key": field, "value": package_dict[field]}
                )

    def _refine_name(self, string):
        """
        ArcGIS API sometimes doesn't provide us with normal titles
        makes small refine to titles
        returns str
        """
        while "_" in string or "  " in string:
            string = string.replace("_", " ").replace("  ", "")

        return string

    def _resources_fetch(self, pkg_data):
        resources = []

        # ArcGIS provides the same resources in different formats
        # We are gonna fetch the 3 most popular
        for fmt in ("CSV", "GeoJSON", "KML"):
            for res in pkg_data["resources"]:
                resource = {}
                resource_url = self._get_resource_url(pkg_data["id"], res["id"], fmt)
                resource["package_id"] = pkg_data["id"]
                resource["url"] = resource_url
                resource["format"] = fmt

                resource["name"] = res.get("name", "") + " ({})".format(fmt)
                resources.append(resource)

        return resources
