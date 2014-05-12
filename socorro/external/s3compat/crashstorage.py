#! /usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/
import boto
import boto.s3.connection
import json
import datetime

from configman import Namespace, class_converter
from socorro.external.crashstorage_base import CrashStorageBase

import socket
import contextlib


#==============================================================================
class DumpReader(object):
    """this class wraps a dump object to embue it with a read method.  This
    allows the dump to be streamed out as "file" upload."""
    #--------------------------------------------------------------------------
    def __init__(self, the_dump):
        self.dump = the_dump

    #--------------------------------------------------------------------------
    def read(self):
        return self.dump


#==============================================================================
class S3CompatCrashStorage(CrashStorageBase):
    """this a crashstorage derivative that just pushes a crash out to a
    Socorro collector waiting at a url"""
    required_config = Namespace()
    required_config.add_option(
        'transaction_executor_class',
        default="socorro.database.transaction_executor."
                "TransactionExecutorWithLimitedBackoff",
        doc='a class that will manage transactions',
        from_string_converter=class_converter
    )
    required_config.add_option(
        'host',
        short_form='h',
        doc="The hostname of the S3 crash storage to submit to",
        default="ceph.dev.phx1.mozilla.com"
    )
    required_config.add_option(
        'access_key',
        short_form='p',
        doc="AWS_ACCESS_KEY_ID",
        default="",
    )
    required_config.add_option(
        'secret_access_key',
        short_form='s',
        doc="AWS_SECRET_ACCESS_KEY",
        default="",
    )
    required_config.add_option(
        'buckets',
        short_form='b',
        doc="How to organize the buckets (default: daily)",
        default="daily"
    )

    operational_exceptions = (
        socket.timeout
    )
    conditional_exceptions = (
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        super(S3CompatCrashStorage, self).__init__(
            config,
            quit_check_callback
        )
        self.transaction = self.config.transaction_executor_class(
            self.config,
            self,
            quit_check_callback
        )

    #--------------------------------------------------------------------------
    def save_raw_crash(self, raw_crash, dumps, crash_id):
        self.transaction(
            self.__class__._submit_crash_via_http_POST,
            raw_crash,
            dumps,
            crash_id
        )

    #--------------------------------------------------------------------------
    def _connect(self):
        return boto.connect_s3(
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_access_key,
            host=self.config.host,
            is_secure=False,
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
        )

    #--------------------------------------------------------------------------
    def _get_bucket(self, timestamp, conn):
        """ Get an S3-like bucket to put things in """
        bucket = None
        bucket_date = datetime.datetime.strptime(timestamp,
            "%Y-%m-%dT%H:%M:%S.%f").strftime('%Y-%m-%d')
        try:
            bucket = conn.create_bucket(bucket_date)
        except boto.exception.s3CreateError:
            # TODO: oops, bucket already taken
            # shouldn't ever happen, but let's handle this
            pass

        return bucket


    #--------------------------------------------------------------------------
    def _submit_crash_via_http_POST(self, raw_crash, dumps, crash_id):
        conn = self._connect()
        bucket = self._get_bucket(raw_crash['submitted_timestamp'], conn)
        storage_key = bucket.new_key(crash_id)
        storage_key.set_contents_from_filename(raw_crash)

        for key, dump in dumps.iteritems():
            if key in (None, '', 'upload_file_minidump'):
                key = 'dump'
            # maybe we have to do key + uuid?
            key = crash_id + key
            storage_key = bucket.new_key(key)
            storage_key.set_contents_from_filename(dump)

        try:
            self.config.logger.debug(
                'submitted %s (original crash_id)',
                raw_crash['uuid']
            )
        except KeyError:
            pass

    # We want this class to be able to participate in retriable transactions.
    # However transactions is a connecton based system and we really don't
    # have a persistant connection associated with an HTTP POST.  So we
    # will use this class itself as its own connection class.  That means
    # that it must have the following methods.  The really important one here
    # is the __call__ method.  That's the key method employed by the
    # transaction class.
    #--------------------------------------------------------------------------
    def commit(self):
        """HTTP POST doesn't support transactions so this silently
        does nothing"""

    #--------------------------------------------------------------------------
    def rollback(self):
        """HTTP POST doesn't support transactions so this silently
        does nothing"""

    #--------------------------------------------------------------------------
    @contextlib.contextmanager
    def __call__(self):
        """this class will serve as its own context manager.  That enables it
        to use the transaction_executor class for retries"""
        yield self

    #--------------------------------------------------------------------------
    def in_transaction(self, dummy):
        """HTTP POST doesn't support transactions, so it is never in
        a transaction."""
        return False

    #--------------------------------------------------------------------------
    def is_operational_exception(self, msg):
        lower_msg = msg.lower()
        if 'timed out' in lower_msg or 'timeout' in lower_msg:
            return True
        return False

    #--------------------------------------------------------------------------
    def force_reconnect(self):
        pass

    #--------------------------------------------------------------------------
    def get_raw_crash(self, crash_id):
        self.transaction(
            self.__class__._get_raw_crash,
            crash_id
        )

    #--------------------------------------------------------------------------
    def _get_raw_crash(self, crash_id):
        conn = self._connect()
        faked_timestamp = datetime.datetime.strptime(
            crash_id[-6:], "%Y%m%d").strftime("%Y-%m-%dT%H:%M:%S.%f")
        bucket = self._get_bucket(faked_timestamp, conn)
        raw_crash = bucket.get_contents_as_string(crash_id)
        return json.loads(raw_crash)

    #--------------------------------------------------------------------------
    def _get_raw_dump(self, crash_id, name=None):
        """ Not implemented yet """
        # TODO: how do we fetch all possible dumps for a crash?
