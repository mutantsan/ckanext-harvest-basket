import logging
from typing import Iterable
import urllib
import json
from urllib.parse import urljoin, urlencode

import ckan.plugins.toolkit as tk
from ckan.lib.helpers import render_markdown
from ckan.lib.munge import munge_tag
from ckan.lib.navl.validators import unicode_safe

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError, SearchError

from ckanext.harvest_basket.harvesters.base_harvester import BasketBasicHarvester


log = logging.getLogger(__name__)


class ODSHarvester(BasketBasicHarvester):
	SRC_ID = "ODS"

	def info(self):
		return {
			"name": "ods",
			"title": "OpenDataSoft",
			"description": "Harvests datasets from remote Opendatasoft portals",
		}

	def gather_stage(self, harvest_job):
		source_url = harvest_job.source.url.strip("/")
		self._set_config(harvest_job.source.config)
		log.info(f"{self.SRC_ID}: gather stage started: {source_url}")

		try:
			pkg_dicts = self._search_datasets(source_url)
		except SearchError as e:
			log.error(f"{self.SRC_ID}: search for datasets failed: {e}")
			self._save_gather_error(
				f"{self.SRC_ID}: unable to search the remote portal for datasets: {source_url}",
				harvest_job,
			)
			return []

		if not pkg_dicts:
			log.error(f"{self.SRC_ID}: search returns empty result.")
			self._save_gather_error(
				f"{self.SRC_ID}: no datasets found at ODS remote portal: {source_url}",
				harvest_job,
			)
			return []

		try:
			package_ids = set()
			object_ids = []

			for pkg_dict in pkg_dicts:
				pkg_id = unicode_safe(pkg_dict["dataset"]["dataset_id"])
				if pkg_id in package_ids:
					log.debug(
						f"{self.SRC_ID}: Discarding duplicate dataset {pkg_id}. ",
						"Probably, due to datasets being changed in process of harvesting"
					)
					continue

				package_ids.add(pkg_id)
				pkg_name: str = pkg_dict["dataset"]["metas"]["default"]["title"]
				log.info(
					f"{self.SRC_ID}: Creating HARVEST object for {pkg_name} | id: {pkg_id}"
				)

				obj = HarvestObject(
					guid=pkg_id, job=harvest_job, content=json.dumps(pkg_dict)
				)
				obj.save()
				object_ids.append(obj.id)

			return object_ids
		except Exception as e:
			log.debug("The error occured during the gather stage: {}".format(e))
			self._save_gather_error(str(e), harvest_job)
			return []

	def _search_datasets(self, source_url):
		"""
		gathering ODS datasets
		returns a list with dicts of datasets metadata
		"""

		pkg_dicts = []
		params = {"rows": 50, "include_app_metas": True}

		max_datasets = tk.asint(self.config.get("max_datasets", 0))

		search_url = urljoin(source_url, "/api/v2/catalog/datasets")
		url = search_url + "?" + urlencode(params)

		while True:
			log.info(f"{self.SRC_ID}: gathering ODS remote dataset: {url}")

			resp = self._make_request(url)
			if not resp:
				continue

			try:
				pkgs_data = json.loads(resp.text)
			except ValueError as e:
				log.debug(
					f"{self.SRC_ID}: can't fetch the metadata. \
					Access denied or JSON object is corrupted"
				)
				return []
			
			for pkg in pkgs_data["datasets"]:
				pkg_dicts.append(pkg)

			url = self._get_next_page_datasets_url(pkgs_data)
			if not url:
				break

			if max_datasets and len(pkg_dicts) > max_datasets:
				break
		
		return pkg_dicts[:max_datasets] if max_datasets else pkg_dicts

	def _get_next_page_datasets_url(self, pkg_dict):
		for link in pkg_dict["links"]:
			if link["rel"] == "next":
				return link["href"]

	def _fetch_resources(self, source_url, resource_urls, pkg_data):
		resources = []

		pkg_id = pkg_data["dataset"]["dataset_id"]

		for res in resource_urls:
			resource = {}

			resource["package_id"] = pkg_id
			resource["url"] = res["href"]
			resource["format"] = res["rel"].upper()

			pkg_meta = pkg_data["dataset"]["metas"]["default"]
			resource["name"] = f"{pkg_meta.get('title', 'Unnamed resource')} ({res['rel']})"

			resources.append(resource)

		# attachments are an additional resources that we can fetch
		atts = pkg_data["dataset"].get("attachments")
		if atts:
			offset = "/api/datasets/1.0/{}/attachments/{}/"
			for att in atts:
				resource = {}
				url = urljoin(source_url, offset.format(pkg_id, att["id"]))

				resource["package_id"] = pkg_id
				resource["url"] = url
				resource["format"] = (att["url"].split(".")[-1]).upper()
				resource["name"] = att.get("title", "")

				resources.append(resource)
		return resources

	def fetch_stage(self, harvest_object):
		self._set_config(harvest_object.source.config)
		source_url = self._get_src_url(harvest_object)
		package_dict = json.loads(harvest_object.content)
		self._pre_map_stage(package_dict, source_url)
		harvest_object.content = json.dumps(package_dict)
		return True

	def _pre_map_stage(self, package_dict: dict, source_url: str):
		package_dict["id"] = package_dict["dataset"]["dataset_id"]
		package_dict["url"] = self._get_dataset_links_data(package_dict)

		meta = package_dict["dataset"]["metas"]["default"]

		package_dict["notes"] = self._description_refine(meta.get("description"))

		res_export_url = self._get_export_resource_url(source_url, package_dict["id"])
		res_links = self._get_all_resource_urls(res_export_url)
		package_dict["resources"] = self._fetch_resources(source_url, res_links, package_dict)

		package_dict["tags"] = self._fetch_tags(meta.get("keyword"))
		package_dict["author"] = meta.get("publisher", "")
		package_dict["title"] = meta.get("title")
		package_dict["type"] = "dataset"

		extra = (
			("language", "Language"),
			("license", "License"),
			("license_url", "License url"),
			("timezone", "Timezone"),
			("parent_domain", "Parent Domain"),
			("references", "References"),
		)

		package_dict["extras"] = []

		if isinstance(meta.get("theme"), Iterable):
			package_dict["extras"].append(
				{"key": "Themes", "value": ", ".join(meta.get("theme"))}
			)

		for field in extra:
			if meta.get(field[0]):
				package_dict["extras"].append({"key": field[1], "value": meta[field[0]]})

	def _get_dataset_links_data(self, pkg_links):
		for link in pkg_links["links"]:
			if link["rel"] == "self":
				return link["href"]

	def _get_export_resource_url(self, source_url, pkg_id):
		offset = "/api/v2/catalog/datasets/{}/exports".format(pkg_id)
		return source_url + offset

	def _get_all_resource_urls(self, res_link):
		if not res_link:
			return []

		resp = self._make_request(res_link)
		if not resp:
			return []

		try:
			content = json.loads(resp.text)
		except ValueError as e:
			log.debug(
				f"{self.SRC_ID}: Can't fetch the metadata. \
				Access denied or JSON object is corrupted"
			)
			return []

		res_links = []
		formats = ("csv", "json", "xls", "geojson")
		for link in content["links"]:
			if link["rel"].lower() not in formats or not link["href"]:
				continue
			res_links.append(link)

		return res_links
