from . import get


def get_auth_functions():
    return {
        "harvest_basket_check_source": get.check_source,
    }
