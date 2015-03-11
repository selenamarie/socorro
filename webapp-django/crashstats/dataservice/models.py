# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import functools
import hashlib
import json
import logging
import os
import stat
import time

from django.conf import settings
from django.core.cache import cache
from configman import (
    configuration,
    # ConfigFileFutureProxy,
    Namespace,
    environment
)
from configman.dotdict import DotDict
from socorro.app.socorro_app import App
from socorro.dataservice.util import (
    classes_in_namespaces_converter,
)


logger = logging.getLogger('dataservice_models')


def memoize(function):
    """Decorator for model methods to cache in memory or the filesystem
    using CACHE_MIDDLEWARE and/or CACHE_MIDDLEWARE_FILES Django config"""

    @functools.wraps(function)
    def memoizer(instance, *args, **kwargs):

        def get_cached_result(key, instance, stringified_args):
            result = cache.get(key)
            if result is not None:
                logger.debug("CACHE HIT %s" % stringified_args)
                return result

            # Didn't find key in middleware_cache, so try filecache
            cache_file = get_cache_filename(key, instance)
            if settings.CACHE_MIDDLEWARE_FILES and os.path.isfile(cache_file):
                # but is it fresh enough?
                age = time.time() - os.stat(cache_file)[stat.ST_MTIME]
                if age > instance.cache_seconds:
                    logger.debug("CACHE FILE TOO OLD")
                    os.remove(cache_file)
                else:
                    logger.debug("CACHE FILE HIT %s" % stringified_args)
                    if instance.expect_json:
                        return json.load(open(cache_file))
                    else:
                        return open(cache_file).read()

            # Didn't find our values in the cache
            return None

        def get_cache_filename(key, instance):
            root = settings.CACHE_MIDDLEWARE_FILES
            if isinstance(root, bool):
                cache_file = os.path.join(
                    settings.ROOT,
                    'models-cache'
                )
            else:
                cache_file = root

            cache_file = os.path.join(cache_file, classname, key)
            cache_file += instance.expect_json and '.json' or '.dump'
            return cache_file

        def refresh_caches(key, instance, result):
            cache.set(key, result, instance.cache_seconds)
            cache_file = get_cache_filename(key, instance)
            if cache_file and settings.CACHE_MIDDLEWARE_FILES:
                if not os.path.isdir(os.path.dirname(cache_file)):
                    os.makedirs(os.path.dirname(cache_file))
                with open(cache_file, 'w') as f:
                    if instance.expect_json:
                        json.dump(result, f, indent=2)
                    else:
                        f.write(result)

        # Check if item is in the cache and call the decorated method if needed
        do_cache = settings.CACHE_MIDDLEWARE and instance.cache_seconds
        if do_cache:
            classname = instance.__class__.__name__
            stringified_args = classname + " " + str(kwargs)
            key = hashlib.md5(stringified_args).hexdigest()
            result = get_cached_result(key, instance, stringified_args)
            if result is not None:
                return result

        # Didn't find it in the cache or not using a cache, so run our function
        result = function(instance, *args, **kwargs)

        if do_cache:
            refresh_caches(key, instance, result)
        return result

    return memoizer


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

settings.DATASERVICE_CONFIG = configuration(
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

magic = {}
for key in settings.DATASERVICE_CONFIG.keys_breadth_first(include_dicts=True):
    if key.startswith('services') and '.' in key:
        local_config = settings.DATASERVICE_CONFIG[key]
        if isinstance(local_config, DotDict):
            service_implementation_class_key = '.'.join((key, 'service_class'))
            impl_class = \
                settings.DATASERVICE_CONFIG[service_implementation_class_key]

            class AService(object):
                implementation_class = impl_class
                required_params = local_config.required_params
                expect_json = local_config.output_is_json
                cache_seconds = local_config.cache_seconds
                uri = local_config.uri
                is_documented = True

                API_BINARY_RESPONSE = local_config.api_binary_response
                API_BINARY_FILENAME = local_config.api_binary_filename
                API_BINARY_PERMISSIONS = local_config.api_binary_permissions
                API_WHITELIST = local_config.api_whitelist
                API_REQUIRED_PERMISSIONS = \
                    local_config.api_required_permissions

                @memoize
                def get(self, **kwargs):
                    impl = self.implementation_class(local_config)
                    result = getattr(impl, local_config.method)(**kwargs)
                    return result

                def get_annotated_params(self):
                    """return an iterator. One dict for each parameter that the
                    class takes.
                    Each dict must have the following keys:
                        * name
                        * type
                        * required
                    """
                    for required, items in (
                        (True, getattr(self, 'required_params', [])),
                        (False, getattr(self, 'possible_params', []))
                    ):
                        for item in items:
                            if isinstance(item, basestring):
                                type_ = basestring
                                name = item
                            elif isinstance(item, dict):
                                type_ = item['type']
                                name = item['name']
                            else:
                                assert isinstance(item, tuple)
                                name = item[0]
                                type_ = item[1]

                            yield {
                                'name': name,
                                'required': required,
                                'type': type_,
                            }

            AService.__name__ = (
                impl_class.__name__
            )
            magic[impl_class.__name__] = AService
