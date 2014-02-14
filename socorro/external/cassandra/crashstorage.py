# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
from Queue import (
    Queue,
    Empty
)

import pycassa

from configman import (
    Namespace,
    class_converter
)
from socorro.external.cassandra.connection_context import (
    ConnectionContextPooled
)
from socorro.external.crashstorage_base import (
    CrashStorageBase,
)


#==============================================================================
class CassandraCrashStorage(CrashStorageBase):
    """This class is an implementation of a Socorro Crash Storage system.
    It is used as a crash aggregation mechanism for raw crashes.  It implements
    the save_raw_crash method as simple aggregates for incoming crashes, and
    saved_processed_crash method as a simple aggregator (for now) of processed
    crash stats.

    Retrying submissions for aggregates is generally frowned upon because there
    is no guarantee that failures are actually failures from the client
    perspective.

    Priority crash reprocessing will be handled like a regular processed crash.

    Reprocessed crashes will not be handled the same way. This is ambiguous
    because it is not clear whether we've reprocessed everything or not. It
    may make sense to truncate stats for a time period and then  submit all
    crashes for a timeslice the way we handle backfilling Postgres matviews.
    """

    required_config = Namespace()
    required_config.add_option(
        'cassandra_class',
        default=ConnectionContextPooled,  # we choose a pooled connection
                                          # because we need thread safe
                                          # connection behaviors
        doc='the class responsible for connecting to Cassandra',
        reference_value_from='resource.cassandra',
    )
    required_config.add_option(
        'transaction_executor_class',
        default="socorro.database.transaction_executor."
                "TransactionExecutor",
        doc='a class that will manage transactions',
        from_string_converter=class_converter,
        reference_value_from='resource.cassandra',
    )
    required_config.add_option(
        'filter_on_legacy_processing',
        default=True,
        doc='toggle for using or ignoring the throttling flag',
        reference_value_from='resource.cassandra',
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        super(CassandraCrashStorage, self).__init__(
            config,
            quit_check_callback=quit_check_callback
        )

        self.config = config

        self.cassandra = config.cassandra_class(config)
        self.transaction = config.transaction_executor_class(
            config,
            self.cassandra,
            quit_check_callback=quit_check_callback
        )

    #--------------------------------------------------------------------------
    def save_processed(self, processed_crash):
        self.config.logger.debug(
            'CassandraCrashStorage saving crash %s', processed_crash.uuid
        )
        self.transaction(self._save_processed_crash_transaction, processed_crash)

    #--------------------------------------------------------------------------
    def _save_processed_crash_transaction(self, connection, processed_crash):

        # Save to counter of incoming crashes
        hour = processed_crash['date_processed'].hour

        new_row_bucket = '{ "hour": "%d", "signature": "%s" }' % (
            int(hour), processed_crash.signature
        )
        connection.connection.insert(new_row_bucket, {hour: 1})
