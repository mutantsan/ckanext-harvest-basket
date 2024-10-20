from __future__ import annotations

import logging
from lxml import etree
from ckanext.spatial.harvesters import CSWHarvester
from ckanext.spatial.lib.csw_client import CswService, CswError, PropertyIsEqualTo
from ckanext.transmute.utils import get_schema
from .base_harvester import BasketBasicHarvester

from requests import utils as request_utils
import owslib.util as ows_util

# from requests_cache import install_cache
# def ff(resp):
#     return resp.url.startswith("https://geonetwork.tern.org.au")
# install_cache("/tmp/csw-harvester", "sqlite", filter_fn=ff)

log = logging.getLogger(__name__)

## this fix included into CKAN v2.11 compatible version of ckanext-spatial. But
## it requires python >= 3.9, so we can drop following lines only after
## upgrading to CKAN v2.11 and newer python
if not hasattr(etree, "_ElementStringResult"):
    setattr(etree, "_ElementStringResult", object())


## A number of CSW services do not support scrapping. The following function is
## used to immitate real user's requests
def default_user_agent():
    return "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


## Same as above, the following function is used to replace User-Agent
## headers. They are hardcoded in the original implementation, so we have to
## replace __code__ object.
def http_post(
    url=None,
    request=None,
    lang="en-US",
    timeout=10,
    username=None,
    password=None,
    auth=None,
    headers=None,
):
    if url is None:
        raise ValueError("URL required")

    u = urlsplit(url)

    headers_ = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Content-type": "text/xml",
        "Accept": "text/xml,application/xml",
        "Accept-Language": lang,
        "Accept-Encoding": "gzip,deflate",
        "Host": u.netloc,
    }

    if headers:
        headers_.update(headers)

    if isinstance(request, dict):
        headers_["Content-type"] = "application/json"
        headers_.pop("Accept")

    rkwargs = {}

    if auth:
        if username:
            auth.username = username
        if password:
            auth.password = password
    else:
        auth = Authentication(username, password)
    if auth.username is not None and auth.password is not None:
        rkwargs["auth"] = (auth.username, auth.password)
    elif auth.auth_delegate is not None:
        rkwargs["auth"] = auth.auth_delegate
    rkwargs["verify"] = auth.verify
    rkwargs["cert"] = auth.cert

    if not isinstance(request, dict):
        return requests.post(url, request, headers=headers_, **rkwargs)
    else:
        return requests.post(url, json=request, headers=headers_, **rkwargs)


request_utils.default_user_agent = default_user_agent
ows_util.http_post.__code__ = http_post.__code__


class BasketCswHarvester(CSWHarvester, BasketBasicHarvester):
    SRC_ID = "CSW"

    def get_package_dict(self, iso_values, harvest_object):
        package_dict = super().get_package_dict(iso_values, harvest_object)

        self.base_context = {"user": self._get_user_name()}
        self._set_config(harvest_object.source.config)

        self._transmute_content(package_dict)
        return package_dict

    def info(self):
        return {
            "name": "basket_csw",
            "title": "Extended CSW Harvester",
            "description": "A server that implements OGC's Catalog Service for the Web (CSW) standard",
        }

    def _setup_csw_client(self, url):
        self.csw = BasketCswService(url)


class BasketCswService(CswService):
    def getidentifiers(
        self,
        qtype=None,
        typenames="csw:Record",
        esn="brief",
        keywords=[],
        limit=None,
        page=10,
        outputschema="gmd",
        startposition=0,
        cql=None,
        **kw,
    ):
        from owslib.catalogue.csw2 import namespaces

        constraints = []
        csw = self._ows(**kw)

        if qtype is not None:
            constraints.append(PropertyIsEqualTo("dc:type", qtype))

        kwa = {
            "constraints": constraints,
            "typenames": typenames,
            "esn": esn,
            "startposition": startposition,
            "maxrecords": page,
            "outputschema": namespaces[outputschema],
            "cql": cql,
            "sortby": self.sortby,
        }
        i = 0
        matches = 0
        while True:
            log.info("Making CSW request: getrecords2 %r", kwa)

            try:
                csw.getrecords2(**kwa)
            except etree.XMLSyntaxError as err:
                log.exception("Cannot parse CSW response")
            else:
                if csw.exceptionreport:
                    err = "Error getting identifiers: %r" % csw.exceptionreport.exceptions
                    # log.error(err)
                    raise CswError(err)

                if matches == 0:
                    matches = csw.results["matches"]

                identifiers = list(csw.records.keys())
                if limit is not None:
                    identifiers = identifiers[: (limit - startposition)]
                for ident in identifiers:
                    yield ident

                if len(identifiers) == 0:
                    break

                i += len(identifiers)
                if limit is not None and i > limit:
                    break

            startposition += page
            if startposition >= (matches + 1):
                break

            kwa["startposition"] = startposition
