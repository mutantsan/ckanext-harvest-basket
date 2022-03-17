from __future__ import annotations
import logging
import geojson
from io import StringIO
from urllib import parse

from ckan.lib.helpers import json
from ckan.plugins import toolkit as tk
from ckan.lib.munge import munge_tag

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError, SearchError

from ckanext.harvest_basket.harvesters.base_harvester import BasketBasicHarvester


log = logging.getLogger(__name__)


class SocrataHarvester(BasketBasicHarvester):
    ALL_PUBLIC_ASSETS: str = "/api/views/"
    SOLR_MAX_STRING_SIZE: int = 32000
    SRC_ID = "Socrata"

    def info(self):
        return {
            "name": "socrata",
            "title": "Socrata",
            "description": "Harvests remote Socrata instances",
        }

    def gather_stage(self, harvest_job):
        source_url = self._get_src_url(harvest_job)

        log.info(f"{self.SRC_ID}: gather stage in progress: {source_url}")

        self._set_config(harvest_job.source.config)

        try:
            pkg_dicts = self._search_datasets(source_url)
        except SearchError as e:
            log.error("Searching for datasets failed: {}".format(e))
            self._save_gather_error(
                "Unable to search the remote Socrata portal for datasets: {}".format(
                    source_url
                ),
                harvest_job,
            )
            return []

        if not pkg_dicts:
            log.error("Searching returns empty result.")
            self._save_gather_error(
                "No datasets found at Socrata remote portal: {}".format(
                    source_url
                ),
                harvest_job,
            )
            return []

        # creating harvest object for each dataset
        try:
            package_ids = set()
            object_ids = []

            for pkg_dict in pkg_dicts:
                if pkg_dict["id"] in package_ids:
                    log.debug(
                        "Discarding duplicate dataset {}. Probably, due to \
								datasets being changed in process of harvesting".format(
                            pkg_dict["id"]
                        )
                    )
                    continue
                package_ids.add(pkg_dict["id"])

                log.info(
                    "Creating HARVEST object for {} | id: {}".format(
                        pkg_dict.get("name", "").encode("utf-8"), pkg_dict["id"]
                    )
                )
                obj = HarvestObject(
                    guid=pkg_dict["id"], job=harvest_job, content=json.dumps(pkg_dict)
                )
                obj.save()
                object_ids.append(obj.id)

            return object_ids

        except Exception as e:
            log.debug(f"The error occured during the gather stage: {e}")
            self._save_gather_error(str(e), harvest_job)
            return []

    def _search_datasets(self, remote_url: str):
        """Fetches the dataset metadata from remote_url

        Args:
                remote_url (str): remote portal URL

        Raises:
                SearchError: raises an error if request to remote portal
                                         has failedrhf

        Returns:
                list[dict]: a list of package dictionaries
        """
        package_list_url = parse.urljoin(remote_url, self.ALL_PUBLIC_ASSETS)

        pkg_dicts = []

        limit = self.config.get("limit", 50)
        max_datasets = tk.asint(self.config.get("max_datasets", 0))

        params = {"page": 1, "limit": limit}

        while True:
            url = f"{package_list_url}?{parse.urlencode(params)}"
            log.debug("Searching for datasets: {}".format(url))

            try:
                resp = self._make_request(url)
            except ContentFetchError as e:
                raise SearchError(
                    "Error sending request to search remote "
                    f"Socrata instance {remote_url} using URL {url}. Error: {e}"
                )

            try:
                pkg_dicts_page = json.loads(resp.text)
            except ValueError as e:
                raise SearchError(f"Response from remote portal was not a JSON: {e}")

            pkg_dicts_page = [p for p in pkg_dicts_page]

            if len(pkg_dicts_page) == 0:
                break

            pkg_dicts.extend(pkg_dicts_page)

            if max_datasets and len(pkg_dicts) > max_datasets:
                break

            params["page"] += 1

        if max_datasets:
            return pkg_dicts[:max_datasets]
        return pkg_dicts

    def _resources_fetch(self, pkg_data):
        resources = []

        # there are several types of resource in socrata portals
        # first of all, main data, which represented in different formats
        # the most common format is CSV, that available almost always
        # except for cases, when dataset represents non-tabular data

        resource = {}
        resource_url = self._get_resource_url(pkg_data)
        resource["package_id"] = pkg_data["id"]
        resource["url"] = resource_url
        resource["format"] = "CSV"
        resource["name"] = pkg_data["name"]

        # if there is no url, skip this resource
        if resource_url:
            resources.append(resource)

        # some datasets has attachments, that we want to harvest too
        # we dont know its format in advance
        if pkg_data.get("metadata") and "attachments" in pkg_data["metadata"]:
            attachments = pkg_data["metadata"]["attachments"]
            for att in attachments:
                resource = {}

                # getting the file download url
                if att.get("assetId"):
                    resource["url"] = self._get_attachment_url(pkg_data["id"], att)
                else:
                    resource["url"] = self._get_attachment_url(
                        pkg_data["id"], att, blob=True
                    )

                resource["package_id"] = pkg_data["id"]
                resource["name"] = att.get("name", "")
                resources.append(resource)

        try:
            # additional endpoints leading to some extra data
            add_endpoint = pkg_data["metadata"]["additionalAccessPoints"][0]
        except KeyError:
            # if there is no endpoints - return fetched resources
            return resources

        for file in add_endpoint["urls"].values():
            resource = {}
            resource["name"] = add_endpoint.get("title", "")
            resource["created"] = self._datetime_refine("")
            resource["last_modified"] = self._datetime_refine("")
            resource["url"] = file
            resources.append(resource)

        return resources

    def _get_resource_url(self, pkg_data: dict):
        """Fetches the resource URL
        Only tabular data are presented in CSV format

        Args:
                pkg_data (dict): remote package data

        Returns:
                str: remote resource URL
        """
        if pkg_data.get("viewType", "") == "tabular":
            api_offset = f"/api/views/{pkg_data['id']}/rows.csv"
            return parse.urljoin(self.source_url, api_offset)

    def _get_attachment_url(self, pkg_id, attachment, blob=False):
        # there are two different keys which can be presented in filepath url
        if not blob:
            api_offset = (
                f"/api/views/{pkg_id}/files/{attachment['assetId']}"
                f"?filename={attachment['filename']}"
            )
            return parse.urljoin(self.source_url, api_offset)

        return parse.urljoin(
            self.source_url, f"/api/assets/{attachment['blobId']}?download=true"
        )

    def _get_pkg_source_url(self, pkg_id):
        """Fetches the package URL on remote portal

        Args:
                        pkg_id (str): package ID

        Returns:
                        str: remote package URL
        """
        api_offset = f"/api/views/metadata/v1/{pkg_id}"
        url = parse.urljoin(self.source_url, api_offset)

        try:
            res = self._make_request(url)
            content = json.loads(res.text)
        except (ValueError, AttributeError) as e:
            log.error(f"Error fetching package url: {e}")
            return ""

        return content.get("dataUri", content.get("webUri", ""))

    def _get_spatial_coverage(self, pkg_id):
        """Fetches the spatial coverage of the remote package
        Because it's impossible to collect correctly all the geojson data
        due to it size (and there is no sense to cut it till it fit the requirements),
        we have decided to collect all the possible points coordinates
        and drop the lines and polygons.

        Args:
                pkg_id (str): remote package ID

        Returns:
                str: spatial coverage
        """

        url = self._get_geojson_data_url(pkg_id)
        geo = self._make_request(url, stream=True)

        # there are two endpoints to get the geojson data
        # if first one failed, try another one
        if not geo:
            url = self._get_geojson_data_url(pkg_id, res=True)

        geo = self._make_request(url, stream=True)

        if not geo:
            return

        # downloading geojson file by chunks into buffer
        # if filesize is too big, then skip it
        # user can define the maxsize in config
        gjson_data = StringIO()

        for number, chunk in enumerate(geo.iter_content(chunk_size=1024 * 32)):
            if chunk:
                gjson_data.write(chunk)
                # you can provide maxsize for geojson file to download
                # if not provided - maxsize is 64 mb
                if number > self.config.get("geojson_maxsize", 2000):
                    log.error("The GeoJSON file is too big for SOLR, skipping...")
                    gjson_data.close()
                    return

        try:
            gjson = geojson.loads(gjson_data.getvalue())["features"]
        except ValueError as e:
            log.error(f"Can't open geojson file, probably it was corrupted: {e}")
            return

        if len(gjson) == 0:
            log.error("No valid spatial data in received geojson")
            return

        geo = geojson.dumps(gjson)

        while len(geo) > self.SOLR_MAX_STRING_SIZE:
            if len(gjson) <= 1:
                log.error("Spatial data is too big. Skipping...")
                return
            gjson = gjson[: len(gjson) - (len(gjson) // 2)]
            geo = geojson.dumps(gjson)

        try:
            points = [
                point["geometry"]["coordinates"]
                for point in gjson
                if point["geometry"] and point["geometry"]["type"] == "Point"
            ]

            geo = geojson.MultiPoint(points)
            if geo["coordinates"]:
                return geojson.dumps(geo)
        except TypeError as e:
            return

    def _get_geojson_data_url(self, pkg_id, res=False):
        """Fetches URL to geojson file for a particular package

        Args:
            pkg_id (str): remote package ID
            res (bool, optional): Flag to use resource endpoint. Defaults to False.

        Returns:
            str: URL to geojson file
        """
        api_offset = f"/api/geospatial/{pkg_id}?method=export&format=GeoJSON"
        if res:
            api_offset = f"/resource/{pkg_id}.geojson"
        return parse.urljoin(self.source_url, api_offset)

    def fetch_stage(self, harvest_object):
        self._set_config(harvest_object.source.config)
        self.source_url = self._get_src_url(harvest_object)
        package_dict = json.loads(harvest_object.content)
        self._pre_map_stage(package_dict, self.source_url)
        harvest_object.content = json.dumps(package_dict)
        return True

    def _pre_map_stage(self, content: dict, source_url: str):
        """Premap stage maps the remote portal meta to CKAN
        if the original field has the same name, the value could be changed
        otherwise, the field stay the same

        Args:
                content (dict): remote package data
        """
        content["resources"] = self._resources_fetch(content)
        content["tags"] = self._fetch_tags(content.get("tags", []))
        content["name"] = self._ensure_name_is_unique(content.get("name", ""))
        content["notes"] = content.get("description", "")
        content["private"] = False
        if "tableAuthor" in content:
            content["author"] = content["tableAuthor"].get("displayName", "")
        content["metadata_created"] = self._datetime_refine(
            content.get("createdAt", "")
        )
        content["metadata_modified"] = self._datetime_refine(
            content.get("indexUpdatedAt", "")
        )
        content["url"] = self._get_pkg_source_url(content["id"])
        content["type"] = "dataset"
        # datasets with map displayType or geo viewType contains geojson data
        # that we can harvest as geojson file
        if content.get("displayType") == "map" or content.get("viewType") == "geo":
            geo = self._get_spatial_coverage(content["id"])
            if geo:
                content["spatial"] = geo

            resource = {}
            resource["package_id"] = content.get("id")
            resource["url"] = self._get_geojson_data_url(content["id"])
            resource["format"] = "GeoJSON"
            resource["name"] = content.get("name", "")

            content["resources"].append(resource)

        # fetching extras from remote portal
        content["extras"] = []
        if "metadata" in content and "custom_fields" in content["metadata"]:
            for metadata in content["metadata"]["custom_fields"].values():
                if isinstance(metadata, str):
                    k, v = metadata
                    content["extras"].append({"key": k, "value": v})
                else:
                    for k, v in metadata.items():
                        content["extras"].append({"key": k, "value": v})

        if "category" in content:
            content["extras"].append({"key": "Category", "value": content["category"]})
