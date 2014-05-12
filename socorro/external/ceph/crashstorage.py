# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/

import boto
import boto.s3.connection
import json
import os
import socket
import datetime

from socorro.external.crashstorage_base import (
    CrashStorageBase,
    CrashIDNotFound
)
from socorro.lib import datetimeutil
from socorro.lib.util import DotDict

from configman import Namespace
from configman.converters import class_converter


#==============================================================================
class CephCrashStorage(CrashStorageBase):
    """This class sends processed crash reports to Ceph
    """

    required_config = Namespace()
    required_config.add_option(
        'transaction_executor_class',
        default="socorro.database.transaction_executor."
        "TransactionExecutorWithLimitedBackoff",
        doc='a class that will manage transactions',
        from_string_converter=class_converter,
    )
    required_config.add_option(
        'host',
        doc="The hostname of the S3 crash storage to submit to",
        default="ceph.dev.phx1.mozilla.com"
    )
    required_config.add_option(
        'port',
        doc="The network port of the S3 crash storage to submit to",
        default=80
    )

    required_config.add_option(
        'access_key',
        doc="AWS_ACCESS_KEY_ID",
        default="",
    )
    required_config.add_option(
        'secret_access_key',
        doc="AWS_SECRET_ACCESS_KEY",
        default="",
    )
    #required_config.add_option(
        #'buckets',
        #doc="How to organize the buckets (default: daily)",
        #default="daily"
    #)

    operational_exceptions = (
        socket.timeout
    )

    conditional_exceptions = ()

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        super(CephCrashStorage, self).__init__(
            config,
            quit_check_callback
        )
        self.transaction = config.transaction_executor_class(
            config,
            self,  # we are our own connection
            quit_check_callback
        )

        # short cuts to external resources - makes testing/mocking easier
        self._connect_to_ceph = boto.connect_s3
        self._calling_format = boto.s3.connection.OrdinaryCallingFormat
        self._CreateError = boto.exception.S3CreateError
        self._open = open


    #--------------------------------------------------------------------------
    def save_raw_crash(self, raw_crash, dumps, crash_id):
        raw_crash_as_string = self._convert_mapping_to_string(raw_crash)
        self._submit_to_ceph(crash_id, "raw_crash", raw_crash_as_string)
        dump_names_as_string = self._convert_list_to_string(dumps.keys())
        self._submit_to_ceph(crash_id, "dump_names", dump_names_as_string)
        for dump_name, dump in dumps.iteritems():
            if dump_name in (None, '', 'upload_file_minidump'):
                dump_name = 'dump'
            self._submit_to_ceph(crash_id, dump_name, dump)

    #--------------------------------------------------------------------------
    def save_processed(self, processed_crash):
        crash_id = processed_crash['uuid']
        processed_crash_as_string = self._convert_mapping_to_string(
            processed_crash
        )
        self._submit_to_ceph(
            crash_id,
            "processed_crash",
            processed_crash_as_string
        )

    #--------------------------------------------------------------------------
    def get_raw_crash(self, crash_id):
        raw_crash_as_string = self._fetch_from_ceph(crash_id, "raw_crash")
        return json.loads(raw_crash_as_string)

    #--------------------------------------------------------------------------
    def get_raw_dump(self, crash_id, name=None):
        if name is None:
            name = 'dump'
        a_dump = self._fetch_from_ceph(crash_id, name)
        return a_dump

    #--------------------------------------------------------------------------
    def get_raw_dumps(self, crash_id):
        dump_names_as_string = self._fetch_from_ceph(crash_id, "dump_names")
        dump_names = self._convert_string_to_list(dump_names_as_string)
        dumps = {}
        for dump_name in dump_names:
            dumps[dump_name] = self._fetch_from_ceph(crash_id, dump_name)
        return dumps

    #--------------------------------------------------------------------------
    def get_raw_dumps_as_files(self, crash_id):
        """the default implementation of fetching all the dumps as files on
        a file system somewhere.  returns a list of pathnames.

        parameters:
           crash_id - the id of a dump to fetch"""
        dumps_mapping = self.get_raw_dumps(crash_id)
        name_to_pathname_mapping = {}
        for a_dump_name, a_dump in dumps_mapping.iteritems():
            dump_pathname = os.path.join(
                self.config.temporary_file_system_storage_path,
                "%s.%s.TEMPORARY%s" % (
                    crash_id,
                    a_dump_name,
                    self.config.dump_file_suffix
                )
            )
            name_to_pathname_mapping[a_dump_name] = dump_pathname
            with self._open(dump_pathname, 'wb') as f:
                f.write(a_dump)
        return name_to_pathname_mapping

    #--------------------------------------------------------------------------
    def get_unredacted_processed(self, crash_id):
        processed_crash_as_string = self._fetch_from_ceph(
            crash_id,
            "processed_crash"
        )
        return json.loads(
            processed_crash_as_string,
            object_hook=DotDict
        )

    #--------------------------------------------------------------------------
    def _submit_to_ceph(self, crash_id, name_of_thing, thing):
        """submit something to ceph.
        """
        if not isinstance(thing, basestring):
            raise Exception('can only submit strings to Ceph')

        conn = self._connect()

        # create/connect to bucket
        try:
            # return a bucket for a given day
            the_day_bucket_name = crash_id[-6:]
            bucket = conn.create_bucket(the_day_bucket_name)
            print bucket
        except self._CreateError:
            # TODO: oops, bucket already taken
            # shouldn't ever happen, but let's handle this
            self.config.logger.error(
                'Ceph bucket creation/connection has failed for %s'
                % the_day_bucket_name,
                exc_info=True
            )
            raise

        key = "%s.%s" % (crash_id, name_of_thing)

        storage_key = bucket.new_key(key)
        storage_key.set_contents_from_string(thing)

    #--------------------------------------------------------------------------
    def _fetch_from_ceph(self, crash_id, name_of_thing):
        """submit something to ceph.
        """
        conn = self._connect()

        # create/connect to bucket
        try:
            # return a bucket for a given day
            the_day_bucket_name = crash_id[-6:]
            bucket = conn.create_bucket(the_day_bucket_name)
        except self._CreateError:
            # TODO: oops, bucket already taken
            # shouldn't ever happen, but let's handle this
            self.config.logger.error(
                'Ceph bucket creation/connection has failed for %s'
                % the_day_bucket_name,
                exc_info=True
            )
            raise

        key = "%s.%s" % (crash_id, name_of_thing)
        thing_as_string = bucket.get_contents_as_string(key)
        return thing_as_string

    #--------------------------------------------------------------------------
    def _connect(self):
        return self._connect_to_ceph(
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_access_key,
            host=self.config.host,
            port=self.config.port,
            is_secure=False,
            calling_format=self._calling_format(),
        )

    #--------------------------------------------------------------------------
    def _convert_mapping_to_string(self, a_mapping):
        self._stringify_dates_in_dict(a_mapping)
        return json.dumps(a_mapping)

    #--------------------------------------------------------------------------
    def _convert_list_to_string(self, a_list):
        return json.dumps(a_list)

    #--------------------------------------------------------------------------
    def _convert_string_to_list(self, a_string):
        return json.loads(a_string)

    #--------------------------------------------------------------------------
    @staticmethod
    def _stringify_dates_in_dict(items):
        for k, v in items.iteritems():
            if isinstance(v, datetime.datetime):
                items[k] = v.strftime("%Y-%m-%d %H:%M:%S.%f")
        return items
