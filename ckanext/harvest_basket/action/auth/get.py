from ckan.plugins import toolkit as tk


def check_source(ctx, data_dict):
    # sysadmins only
    return {"success": False}


@tk.auth_allow_anonymous_access
@tk.chained_auth_function
def package_search(next_, context, data_dict):
    view, action = tk.get_endpoint()
    allow_anon: bool = tk.asbool(tk.config.get("ckanext.harvest_basket.allow_anonymous", 1))

    if not allow_anon and view == "harvest" and not any((tk.g.user, tk.g.userobj)):
        return tk.abort(403, tk._('Anonymous user cannot see this page'))
    return next_(context, data_dict)


def update_config(ctx, data_dict):
    # sysadmins only
    return {"success": False}