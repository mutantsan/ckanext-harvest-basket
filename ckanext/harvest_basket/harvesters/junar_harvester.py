import logging
from urllib.parse import urljoin

from ckan.lib.helpers import json
from ckan.lib.munge import munge_tag

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters.ckanharvester import SearchError, ContentFetchError
from ckanext.harvest_basket.harvesters import DKANHarvester


log = logging.getLogger(__name__)


class JunarHarvester(DKANHarvester):
	def info(self):
		return {
			"name": "junar",
			"title": "Junar",
			"description": (
				"Harvests datasets from Junar portals. "
				"Provide the `auth_key` before start. "
				"The key could be acquired at the target Junar portal"
			)
		}
	
	def gather_stage(self, harvest_job):
		self.source_type = "Junar"
		source_url = harvest_job.source.url.strip("/")
		log.info(f"{self.source_type}: Junar gather stage in progress: {source_url}")
  
		self._set_config(harvest_job.source.config)

		try:
			pkg_dicts = self._search_for_datasets(source_url)
		except SearchError as e:
			log.error(f"{self.source_type}: searching for datasets failed: {e}")
			self._save_gather_error(
				f"{self.source_type}: unable to search the remote Junar portal for datasets: {source_url}",
				harvest_job
			)
			return []

		if not pkg_dicts:
			log.error(f"{self.source_type}: searching returns empty result.")
			self._save_gather_error(
				f"{self.source_type}: no datasets found at Junar remote portal: {source_url}",
				harvest_job
			)
			return []
		
		try:
			object_ids = []

			for pkg_dict in pkg_dicts:
				log.info(
					f"{self.source_type}: Creating HARVEST object "
	 				f"for {pkg_dict['title']} | guid: {pkg_dict['guid']}"
				)

				obj = HarvestObject(guid=pkg_dict["guid"],
									job=harvest_job,
									content=json.dumps(pkg_dict))
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
		url = self._get_all_resources_data_url(source_url)
		
		max_datasets = int(self.config.get("max_datasets", 0))

		while True:
			log.info(f"{self.source_type}: Gathering remote dataset: {url}")

			content = self._get_content(url)
			if not content:
				log.debug(
        			f"{self.source_type}: Remote portal doesn't provide resources API. "
					"Changing API endpoint")
				url = self._get_all_datasets_json_url(source_url)
				continue

			pkgs_data = json.loads(content)
			
			package_ids = set()
			for pkg in pkgs_data["results"]:
				if "/dashboards/" in pkg["link"]:
					# we can skip the dashboards, cause it's just a "collection"
					# of resources that we will get anyway
					break
				# Junar API returns duplicated IDs
				# They put the same resource in defferent "types"
				if pkg["guid"] in package_ids:
					log.debug(f"{self.source_type}: Discarding duplicate dataset {pkg['guid']}.")
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
		auth_key = self.config.get("auth_key")
		return urljoin(source_url, f"/api/v2/datasets.json/?auth_key={auth_key}&limit=50")

	def _get_all_resources_data_url(self, source_url):
		auth_key = self.config.get("auth_key")
		return urljoin(source_url, f"/api/v2/resources/?auth_key={auth_key}&limit=50")
