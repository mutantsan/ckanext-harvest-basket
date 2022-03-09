from . import get


def get_actions():
    return {
        "harvest_basket_check_source": get.check_source,
        "harvest_basket_update_config": get.update_config
    }
