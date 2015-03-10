# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from django.conf import settings
from configman import (
    configuration,
    # ConfigFileFutureProxy,
    Namespace,
    environment
)
from socorro.app.socorro_app import App

from socorro.dataservice.util import (
    classes_in_namespaces_converter,
)

from collections import Mapping, Iterable


SERVICES_LIST = ('socorro.external.postgresql.bugs_service.Bugs',)

# Allow configman to dynamically load the configuration and classes
# for our API dataservice objects
def_source = Namespace()
def_source.namespace('services')
def_source.services.add_option(
    'service_list',
    doc='a list of classes that represent services to expose',
    default=','.join(SERVICES_LIST),
    from_string_converter=classes_in_namespaces_converter('service_class')
)

settings.DATASERVICE_CONFIG = settings.DATASERVICE_CONFIG(
    definition_source=[
        def_source,
        App.get_required_config(),
    ],
    values_source_list=[
        settings.DATASERVICE_CONFIG_BASE,
        # ConfigFileFutureProxy,
        environment
    ]
)

for key in settings.DATASERVICE_CONFIG.keys_breadth_first():
    if key.startswith('services') and  '.' in key:
        local_config = settings.DATASERVICE_CONFIG[key]
        if isinstance(local_config, Namespace):
            service_implementation_class_key = '.'.join((key, 'service_class'))
            impl_class = settings.DATASERVICE_CONFIG[service_implementation_class_key]
            class AService(object):
                implementation_class = impl_class
                required_params = local_config.required_params
                expect_json = local_config.output_is_json
                API_WHITELIST = local_config.api_whitelist
                
                def get(self, **kwargs):
                    impl_args = DotDict()
                    impl = implementation_class(local_config)
                    result = impl.get(**kwargs)
                    return result
                
            AService.__name__ = (
                impl_class.__name__
            )