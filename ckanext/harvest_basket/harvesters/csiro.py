from __future__ import annotations

import contextlib
import logging
import json
import uuid
from typing import Any, Iterable

from ckan.lib.munge import munge_name, munge_tag
from ckan.logic import ValidationError
import ckan.plugins.toolkit as tk

from ckan import model
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters.ckanharvester import SearchError
from ckanext.spatial import harvesters

from ckanext.harvest_basket.harvesters.base_harvester import BasketBasicHarvester


log = logging.getLogger(__name__)


# from requests_cache import install_cache
# def ff(resp):
#     return resp.url.startswith("https://data.csiro.au/")
# install_cache("/tmp/csw-harvester", "sqlite", filter_fn=ff)


class CsiroHarvester(BasketBasicHarvester):
    SRC_ID = "CSIRO"

    def info(self):
        return {
            "name": "csiro",
            "title": "CSIRO",
            "description": "Harvests datasets from CSIRO DAP",
        }

    def gather_stage(self, harvest_job):
        source_url: str = harvest_job.source.url.strip("/")
        self._set_config(harvest_job.source.config)
        log.info(f"{self.SRC_ID}: gather stage started: {source_url}")

        object_ids = []
        try:
            for record in self._search_datasets(source_url):
                identifier = munge_name(
                    "_".join(
                        [record["id"]["identifierType"], record["id"]["identifier"]]
                    )
                )
                log.info(
                    "%s: Creating HARVEST object for %s",
                    self.SRC_ID,
                    identifier,
                )

                guid = uuid.uuid5(uuid.NAMESPACE_DNS, identifier)
                obj = HarvestObject(
                    guid=identifier, job=harvest_job, content=json.dumps(record)
                )
                obj.save()
                object_ids.append(obj.id)

        except SearchError:
            log.exception("%s: search for datasets failed", self.SRC_ID)
            self._save_gather_error(
                f"{self.SRC_ID}: unable to search the remote portal for datasets: {source_url}",
                harvest_job,
            )

        if not object_ids:
            log.error("%s: search returns empty result.", self.SRC_ID)
            self._save_gather_error(
                f"{self.SRC_ID}: no datasets found at ODS remote portal: {source_url}",
                harvest_job,
            )

        return object_ids

    def _search_datasets(self, url: str) -> Iterable[dict[str, Any]]:
        next_url = url + "/collections.json?rpp=100"
        while True:
            if not (resp := self._make_request(next_url)):
                break

            data = resp.json()
            yield from data["dataCollections"]

            if next_info := data["next"]:
                next_url = next_info["href"]
            else:
                break

    def fetch_stage(self, harvest_object):
        self._set_config(harvest_object.source.config)
        source_url = self._get_src_url(harvest_object)
        package_dict = json.loads(harvest_object.content)
        url = f"{source_url}/collections/{package_dict['id']['identifier']}.json"
        log.debug("Fetch %s", url)

        if not (resp := self._make_request(url)):
            return False
        metadata = resp.json()

        if "data" in metadata:
            with contextlib.suppress(tk.ValidationError):
                if resp := self._make_request(metadata["data"] + ".json"):
                    data = resp.json()
                    metadata["files"] = data["file"]

        harvest_object.content = json.dumps(metadata)
        return True

    def import_stage(self, harvest_object):
        self.base_context = {
            "model": model,
            "session": model.Session,
            "user": self._get_user_name(),
        }

        self._set_config(harvest_object.source.config)

        package_dict = {}

        data = json.loads(harvest_object.content)

        log.debug("Import %s", data["id"])

        package_dict["name"] = munge_name(
            "_".join([data["id"]["identifierType"], data["id"]["identifier"]])
        )
        package_dict["id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, package_dict["name"]))

        package_dict["title"] = data.pop("title")
        package_dict["notes"] = data.pop("description")
        package_dict["tags_string"] = [
            {"name": munge_tag(tag)} for tag in data["keywords"].split(";")
        ]

        package_dict["author"] = data.pop("leadResearcher")

        package_dict["resources"] = [
            {
                "name": item["filename"],
                "size": item["fileSize"],
                "url": item["link"]["href"],
                "format": item["link"]["mediaType"],
                "extras": [{"key": k, "value": v} for k, v in item.items()],
            }
            for item in data.pop("files", [])
        ]

        package_dict["extras"] = [
            {"key": key, "value": value} for key, value in data.items()
        ]
        harvest_object.content = json.dumps(package_dict)
        super().import_stage(harvest_object)
