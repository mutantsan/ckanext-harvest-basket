import logging
import requests
from datetime import datetime as dt
from urllib import parse
from html import unescape
from time import sleep
from dateutil import parser

from html2markdown import convert

from ckan import model
from ckan.lib.helpers import json
from ckan.lib.munge import munge_tag
from ckan.plugins import toolkit as tk

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters.base import HarvesterBase
from ckanext.harvest.harvesters.ckanharvester import ContentFetchError, SearchError


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

		max_datasets = int(self.config.get("max_datasets", 0))
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

			if max_datasets and len(pkg_dicts) == max_datasets:
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

	def fetch_stage(self, harvest_object):
		content = json.loads(harvest_object.content)
		content['resources'] = self._fetch_resources(
				content.get('resources'),
				content.get('id'))
		content['tags'] = self._fetch_tags(content.get('tags'))

		content['private'] = False
		content['state'] = content.get('state', u'active').lower()
		content['type'] = content.get('type', u'dataset').lower()

		content['metadata_created'] = self._datetime_refine(
					content.get('metadata_created'))
		content['metadata_modified'] = self._datetime_refine(
					content.get('metadata_modified'))

		content['notes'] = self._description_refine(content.get('notes'))

		for key in ['groups', 'log_message', 'revision_timestamp', 'creator_user_id']:
			content.pop(key, None)

		content = json.dumps(content)
		harvest_object.content = content

		return True

	def _fetch_tags(self, tags_list):
		tags = []
		if tags_list:
			for t in tags_list:
				tag = {}
				tag['name'] = munge_tag(t['name'])
				tag['id'] = t.get('id', u'')
				tag['state'] = u'active'
				tag['display_name'] = tag['name']

				tags.append(tag)

		return tags

	def _fetch_resources(self, resource_list, pkg_id):
		resources = []
		if resource_list:
			for res in resource_list:
				resource = {}

				resource['description'] = self._description_refine(
					res.get('description'))
				resource['id'] = res.get('id', '')
				resource['format'] = res.get('format', '').upper()

				resource['last_modified'] = self._datetime_refine(
					res.get('last_modified'))
				resource['created'] = self._datetime_refine(
					res.get('created'))

				resource['mimetype'] = res.get('mimetype')

				resource['name'] = res.get('name', res.get('title', u''))
				resource['package_id'] = pkg_id
				resource['size'] = self._size_refine(res.get('size', None))
				resource['state'] = res.get('state', 'active').lower()
				resource['url'] = res.get('url', '')

				resource.pop('revision_id', None)

				resources.append(resource)

		return resources

	def _description_refine(self, string):
		if not string:
			return ""
		string = unescape(string)
		return convert(string)

	def _datetime_refine(self, string):
		now = dt.now().isoformat()
		
		# if date stored as timestamp
		try: 
			if string and int(string):
				formated_data = dt.fromtimestamp(string).isoformat()
				return formated_data
		except ValueError:
			return now

		# if date stored as string
		if string:
			# some portals returns modified_data with this substring
			if string.startswith('Date changed'):
				string = string[13:]

			try:
				formated_data = parser.parse(string, ignoretz=True).isoformat()
			except ValueError:
				formated_data = now

			return formated_data
		return now

	def _size_refine(self, size_string):
		# if resource size stored as int
		try:
			if size_string and int(size_string):
				return int(size_string)
		except ValueError:
			pass

		if size_string:
			# bytes don't need convertion
			if 'byte' in size_string.lower():
				return size_string.split(' ')[0]

			metric_sys = {'kb': 1e3,
							'mb': 1e6,
							'gb': 1e9}
			value, metric = size_string.lower().split(' ')
			return int(float(value) * metric_sys[metric])

		return 0

	def import_stage(self, harvest_object):
		self.base_context = {
			'model': model,
			'session': model.Session,
			'user': self._get_user_name()
 		}

		if not harvest_object:
			log.error('No harvest object received')
			return False

		if harvest_object.content is None:
			log.error(f'Empty content for object {harvest_object.id}: {harvest_object}')
			return False

		package_dict = json.loads(harvest_object.content)
		self.transmute_data(package_dict)
		
		if package_dict.get('type') == 'harvest':
			log.info('Remote dataset is a harvest source, ignoring...')
			return True

		default_extras = self.config.get('default_extras', {})

		def get_extra(key, package_dict):
			for extra in package_dict.get('extras', []):
				if extra['key'] == key:
					return extra

		if default_extras:
			# you can disable extras override by defining override_extras to True in config
			override_extras = self.config.get('override_extras', False)
			if 'extras' not in package_dict:
				package_dict['extras'] = []

			for k, v in default_extras.items():
				existing_extra = get_extra(k, package_dict)
				if existing_extra and not override_extras:
					continue
				if existing_extra:
					package_dict['extras'].remove(existing_extra)
				
				if isinstance(v, str):
					v = v.format(
						harvest_source_id=harvest_object.job.source.id,
						harvest_source_url=harvest_object.job.source.url.strip(
							'/'),
						harvest_source_title=harvest_object.job.source.title,
						harvest_job_id=harvest_object.job.id,
						harvest_object_id=harvest_object.id,
						dataset_id=package_dict['id'])

				package_dict['extras'].append({'key': k, 'value': v})

		# Local harvest source organization
		source_dataset = tk.get_action('package_show')(
			self.base_context, {'id': harvest_object.source.id})
		local_org = source_dataset.get('owner_org')

		package_dict['owner_org'] = local_org

		try:
			result = self._create_or_update_package(package_dict,harvest_object,package_dict_form='package_show')
			return result
		except tk.ValidationError as e:
			log.error('Invalid package with GUID {}: {}'.format(
				(harvest_object.guid, e.error_dict),
				harvest_object))
		except Exception as e:
			self._save_object_error(str(e), harvest_object, 'Import')

	def transmute_data(self, data):
		transmute_schema = self.config.get('tsm_schema')
		
		if transmute_schema:
			tk.get_action('tsm_transmute')(self.base_context, {
				"data": data,
				"schema": transmute_schema
			})
