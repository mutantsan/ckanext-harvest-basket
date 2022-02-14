import logging
import requests
from urllib import parse
from time import sleep

from ckan.lib.helpers import json
from ckan.plugins import toolkit as tk

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.harvesters.ckanharvester import (
	ContentFetchError,
	SearchError,
)


log = logging.getLogger(__name__)


class DKANHarvester(HarvesterBase):
	PACKAGE_LIST: str = "/api/3/action/package_list"
	PACKAGE_SHOW: str = "/api/3/action/package_show"

	def info(self):
		return {
			"name": "dkan",
			"title": "DKAN",
			"description": "Harvests remote DKAN instances",
		}

	def gather_stage(self, harvest_job):
		source_url = harvest_job.source.url.strip("/")
		log.info(f"DKAN: Starting gather_stage {source_url}")

		self._set_config(harvest_job.source.config)
		log.info(f"DKAN: Using config: {self.config}")

		try:
			pkg_dicts = self._search_for_datasets(source_url)
		except SearchError as e:
			log.error(f"DKAN: Searching for datasets failed: {e}")
			self._save_gather_error(
				f"DKAN: Unable to search remote portla for datasets: {source_url}",
				harvest_job,
			)
			return []

		if not pkg_dicts:
			self._save_gather_error(
				f"DKAN: No datasets found at remote portal: {source_url}",
				harvest_job,
			)
			return []

		package_ids = set()
		object_ids = []

		for pkg_dict in pkg_dicts:

			if pkg_dict["id"] in package_ids:
				log.debug(
					f"DKAN: Discarding duplicate dataset {pkg_dict['id']}. "
					"Probably, due to datasets being changed in process of harvesting"
				)
				continue

			package_ids.add(pkg_dict["id"])

			log.info(
				f"DKAN: Creating harvest_object for {pkg_dict.get('name', '')} {pkg_dict['id']}"
			)
   
			try:
				obj = HarvestObject(
					guid=pkg_dict["id"], job=harvest_job, content=json.dumps(pkg_dict)
				)
				obj.save()
				object_ids.append(obj.id)
			except TypeError as e:
				log.debug(f"DKAN: The error occured during the gather stage: {str(e)}")
				self._save_gather_error(str(e), harvest_job)
				continue
		
		return object_ids

	def _set_config(self, config_str):
		if config_str:
			self.config = json.loads(config_str)
		else:
			self.config = {}
   
	def _search_for_datasets(self, remote_url):
		package_list_url = remote_url + self.PACKAGE_LIST

		pkg_dicts = []

		package_names = self._get_package_names(package_list_url)
		package_names = json.loads(package_names)["result"]

		max_datasets = int(self.config.get("max_datasets", 100))
		delay = int(self.config.get("delay", 0))

		for package_name in set(package_names):
			url = f"{remote_url}{self.PACKAGE_SHOW}?{parse.urlencode({'id': package_name})}"
			log.debug(f"DKAN: Searching for dataset: {url}")

			try:
				content = self._get_content(url)
			except ContentFetchError as e:
				raise SearchError(
					"DKAN: Error sending request to the remote "
     				f"instance {remote_url} using URL {url}. Error: {e}"
				)

			try:
				package_dict_page = json.loads(content)["result"]
			except ValueError as e:
				log.error(f"DKAN: Response JSON doesn't contain result, {e}")
				continue

			# some portals return a dict as result, not a list
			if "id" in package_dict_page:
				pkg_dict = []
				pkg_dict.append(package_dict_page)
				package_dict_page = pkg_dict

			pkg_dicts.extend(package_dict_page)

			if len(pkg_dicts) == max_datasets:
				break

			# to avoid ban for frequent requests
			# you can use delay parameter in config
			if delay > 0:
				sleep(delay)
				log.info(f"DKAN: Sleeping for {delay} second(s)")

		return pkg_dicts

	def _get_package_names(self, url):
		http_request = requests.get(url)

		if http_request.status_code == 200:
			return http_request.text

		log.error("Bad response from remote portal")
		raise ContentFetchError(
			"Request status_code: {}".format(http_request.status_code)
		)

	def _get_content(self, url: str) -> str:
		resp = None

		try:
			resp = requests.get(url)
		except requests.exceptions.HTTPError as e:
			log.error("The HTTP error happend during request {}".format(e))
		except requests.exceptions.ConnectTimeout as e:
			log.error('Connection timeout: {}'.format(e))
		except requests.exceptions.ConnectionError as e:
			log.error("The Connection error happend during request {}".format(e))
		except requests.exceptions.RequestException as e:
			log.error("The Request error happend during request {}".format(e))

		if resp and resp.status_code == 200:
			return resp.text
		elif resp and resp.status_code != 200:
			log.error(f'Bad response from remote portal: {resp.status_code}, {resp.reason}')

		# Sleep is for cases when we get refused connection due to multiple requests
		sleep(5)
		return ""