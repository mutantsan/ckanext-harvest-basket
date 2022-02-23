from . import get


def get_actions():
    return {
        "harvest_basket_check_source": get.check_source,
    }
