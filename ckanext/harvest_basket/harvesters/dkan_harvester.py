import logging

from ckanext.harvest.harvesters.base import HarvesterBase


log = logging.getLogger(__name__)


class DKANHarvester(HarvesterBase):
    def info(self):
        return {
            "name": "dkan",
            "title": "DKAN",
            "description": "Harvests remote DKAN instances",
        }
