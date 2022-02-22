from typing import Optional
import requests
import logging
from datetime import datetime as dt
from dateutil import parser

import ckan.plugins.toolkit as tk

from ckanext.harvest.harvesters.ckanharvester import ContentFetchError
from ckanext.harvest.harvesters.base import HarvesterBase


log = logging.getLogger(__name__)


class BasketBasicHarvester(HarvesterBase):
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
            if string.startswith("Date changed"):
                string = string[13:]

            try:
                formated_data = parser.parse(string, ignoretz=True).isoformat()
            except ValueError:
                formated_data = now

            return formated_data
        return now
    
    def _make_request(
        self, url: str, stream: bool = False
    ) -> Optional[requests.Response]:
        resp = None
        err_msg = ""

        try:
            resp = requests.get(url, stream=stream)
        except requests.exceptions.HTTPError as e:
            err_msg = f"The HTTP error happend during request {e}"
            log.error(err_msg)
        except requests.exceptions.ConnectTimeout as e:
            err_msg = f"Connection timeout: {e}"
            log.error(err_msg)
        except requests.exceptions.ConnectionError as e:
            err_msg = f"The Connection error happend during request {e}"
            log.error(err_msg)
        except requests.exceptions.RequestException as e:
            err_msg = f"The Request error happend during request {e}"
            log.error(err_msg)

        if resp is None:
            raise tk.ValidationError(
                {self.source_type: err_msg}
            )

        if resp.status_code == 200:
            return resp

        err_msg = (
            f"{self.source_type}: bad response from remote portal: "
            f"{resp.status_code}, {resp.json().get('description') or resp.reason}"
        )
        log.error(err_msg)
        raise tk.ValidationError({self.source_type: err_msg})


    def make_checkup(self, source_url: str, source_name: str, config: dict):
        """Makes a test fetch of 1 dataset from the remote source


        Args:
            source_url (str): remote portal URL
            config (dict): config dictionary with some options to adjust
                            harvester (e.g schema, max_datasets, delay)

        Raises:
            tk.ValidationError: raises validation error if the fetch failed
                                returns all the information about endpoint
                                and occured error

        Returns:
            dict: must return a remote dataset meta dict
        """
        self.config = config
        self.config.update(
            {
                "max_datasets": 1,
            }
        )

        self.source_url = source_url
        self.source_type = source_name.title()

        try:
            pkg_dicts = self._search_for_datasets(source_url)
        except Exception as e:
            raise tk.ValidationError(
                "Checkup failed. Check your source URL \n"
                f"Endpoint we used: {getattr(self, 'url', '')} \n"
                f"Error: {e}"
            )

        if not pkg_dicts:
            return "No datasets found on remote portal: {source_url}"

        self._pre_map_stage(pkg_dicts[0])
        return pkg_dicts[0]
