The `harvest_basket` extension comes with a few custom **harvesters** for different data portals.
The list of available harvesters, that must be enabled with `ckan.plugins` in your config:
- `dkan_harvester`
- `junar_harvester`
- `socrata_harvester`
- `arcgis_harvester`

This extension also adds some features to extend the basic harvester:
1. Source checkup preview. During the source creation stage the harvester will try to access the remote portal and harvest one dataset to check if it's accessible or not. (TODO: the source checkup interface will be documented soon).
2. Restriction for anonymous users to visit harveser pages

## Installation


To install ckanext-harvest-basket:

1. Activate your CKAN virtual environment, for example:

     . /usr/lib/ckan/default/bin/activate

2. Clone the source and install it on the virtualenv

    git clone https://github.com/mutantsan/ckanext-harvest-basket.git
    cd ckanext-harvest-basket
    pip install -e .
	pip install -r requirements.txt

3. Add `harvest_basket` to the `ckan.plugins` setting in your CKAN
   config file (by default the config file is located at
   `/etc/ckan/default/ckan.ini`).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu:

     sudo service apache2 reload


## Config settings
Available config options:

	# You can disallow accessing harvester pages for anonymous users.
	# (optional, default: 1).
	ckanext.harvest_basket.allow_anonymous = 0


## Developer installation

To install ckanext-harvest-basket for development, activate your CKAN virtualenv and
do:

    git clone https://github.com/mutantsan/ckanext-harvest-basket.git
    cd ckanext-harvest-basket
    python setup.py develop
    pip install -r requirements.txt


## Tests

To run the tests, do:

    pytest --ckan-ini=test.ini

## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)
