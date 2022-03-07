from . import get
from .get import package_search


def get_auth_functions():
    return {
        "harvest_basket_check_source": get.check_source,
        "package_search": package_search,
    }
