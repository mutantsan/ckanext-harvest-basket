from . import get


def get_auth_functions():
    return {
        "harvest_basket_check_source": get.check_source,
        "package_search": get.package_search,
        "harvest_basket_update_config": get.update_config
    }
