# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import contextlib
import json
import os
import socket
import urllib2
import poster
poster.streaminghttp.register_openers()

from socorro.external.crashstorage_base import (
    CrashStorageBase,
    CrashIDNotFound
)
from socorro.lib import datetimeutil

from configman import Namespace
from configman.converters import class_converter

#==============================================================================
class CephCrashStorage(CrashStorageBase):
    """This class sends processed crash reports to elasticsearch. It handles
    indices creation and type mapping. It cannot store raw dumps or raw crash
    reports as Socorro doesn't need those in elasticsearch at the moment.
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
        'host_submission_url_part',
        default="http://somewhere.over.rainbow",
        doc='the host part of the submission url',
    )
    required_config.add_option(
        'raw_crash_target_submission_url_part',
        default="some/service",
        doc='the targe part of the submission url',
    )
    required_config.add_option(
        'processed_crash_target_submission_url_part',
        default="some/service",
        doc='the targe part of the submission url',
    )
    required_config.add_option(
        'get_raw_crash_url_part',
        default="some/service",
        doc='the targe part of the submission url',
    )
    required_config.add_option(
        'get_dumps_crash_url_part',
        default="some/service",
        doc='the targe part of the submission url',
    )
    required_config.add_option(
        'get_processed_crash_url_part',
        default="some/service",
        doc='the targe part of the submission url',
    )

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
        self.raw_submission_url = '/'.join(
            config.host_submission_url_part,
            config.raw_crash_target_submission_url_part,
        )
        self.processed_submission_url = '/'.join(
            config.host_submission_url_part,
            config.processed_crash_target_submission_url_part,
        )
        self.get_raw_crash_url = '/'.join(
            config.host_submission_url_part,
            config.get_raw_crash_url_part,
        )
        self.get_dump_url = '/'.join(
            config.host_submission_url_part,
            config.get_dumps_crash_url_part,
        )
        self.get_processed_url = '/'.join(
            config.host_submission_url_part,
            config.get_processed_crash_url_part,
        )

    #--------------------------------------------------------------------------
    def save_raw_crash(self, raw_crash, dumps, crash_id):
        # assuming http POST
        try:
            for dump_name, dump_pathname in dumps.iteritems():
                if not dump_name:
                    dump_name = self.config.source.dump_field
                raw_crash[dump_name] = open(dump_pathname, 'rb')
            datagen, headers = poster.encode.multipart_encode(raw_crash)
            request = urllib2.Request(
                self.raw_submission_url,
                datagen,
                headers
            )
            submission_response = self.transaction(
                self.__class__._submit_crash_to_ceph,
                request
            )
            try:
                self.config.logger.debug(
                    'submitted %s (original crash_id)',
                    raw_crash['uuid']
                )
            except KeyError:
                pass
            self.config.logger.debug(
                'submission response: %s',
                submission_response
            )
        finally:
            for dump_name, dump_pathname in dumps.iteritems():
                if "TEMPORARY" in dump_pathname:
                    os.unlink(dump_pathname)

    #--------------------------------------------------------------------------
    def save_processed(self, processed_crash):
        crash_id = processed_crash['uuid']
        sanitized_processed_crash = self.sanitize(processed_crash)  # jsonify?
        try:
            datagen, headers = poster.encode.multipart_encode(
                sanitized_processed_crash
            )
            request = urllib2.Request(
                self.processed_submission_url,
                datagen,
                headers
            )
            submission_response = self.transaction(
                self.__class__._submit_crash_to_ceph,
                request
            )
            try:
                self.config.logger.debug(
                    'submitted %s (original crash_id)',
                    raw_crash['uuid']
                )
            except KeyError:
                pass
            self.config.logger.debug(
                'submission response: %s',
                submission_response
            )
        except KeyError, x:
            if x == 'uuid':
                raise CrashIDNotFound
            raise

    #--------------------------------------------------------------------------
    def get_raw_crash(self, crash_id):
        pass

    #--------------------------------------------------------------------------
    def get_raw_dump(self, crash_id, name=None):
        pass

    #--------------------------------------------------------------------------
    def get_raw_dumps(self, crash_id):
        pass

    #--------------------------------------------------------------------------
    def get_raw_dumps_as_files(self, crash_id):
        pass

    #--------------------------------------------------------------------------
    def get_unredacted_processed(self, crash_id):
        pass

    #--------------------------------------------------------------------------
    def _submit_to_ceph(self, request):
        """submit a crash report to ceph.
        """
        try:
            return urllib2.urlopen(request).read().strip()
        except Exception:
            self.logger.critical(
                'Submission to ceph failed for %s',
                crash_id,
                exc_info=True
            )
            raise

    #--------------------------------------------------------------------------
    @staticmethod
    def _stringify_dates_in_dict(items):
        for k, v in items.iteritems():
            if isinstance(v, datetime.datetime):
                items[k] = v.strftime("%Y-%m-%d %H:%M:%S.%f")
        return items
