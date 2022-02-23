import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from ckanext.harvest_basket.action.logic import get_actions
from ckanext.harvest_basket.action.auth import get_auth_functions


class HarvestBasketPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IAuthFunctions)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_public_directory(config_, "public")
        toolkit.add_resource("assets", "harvest_basket")

    # IActions

    def get_actions(self):
        return get_actions()

    # IAuthFunctions

    def get_auth_functions(self):
        return get_auth_functions()
