import unittest

import datetime
from mock import Mock, MagicMock

from socorro.external.cassandra.crashstorage import (
    CassandraCrashStorage,
)
from socorro.lib.util import DotDict
from socorro.external.crashstorage_base import Redactor


class TestCrashStorage(unittest.TestCase):

    def _setup_config(self):
        config = DotDict()
        config.transaction_executor_class = Mock()
        config.logger = Mock()
        config.cassandra_class = MagicMock()
        config.filter_on_legacy_processing = True
        config.redactor_class = Redactor
        config.forbidden_keys = Redactor.required_config.forbidden_keys.default
        return config

    def test_constructor(self):
        config = self._setup_config()
        crash_store = CassandraCrashStorage(config)
        config.cassandra_class.assert_called_once_with(config)
        config.transaction_executor_class.assert_called_once_with(
            config,
            crash_store.cassandra,
            quit_check_callback=None
        )

    def test_save_raw_crash_normal(self):
        config = self._setup_config()
        crash_store = CassandraCrashStorage(config)

        raw_crash = DotDict()
        raw_crash.submitted_timestamp = "2014-02-18 00:00:00+00"
        # test for "legacy_processing" missing from crash
        crash_store.save_raw_crash(
            raw_crash=raw_crash,
            dumps=DotDict(),
            crash_id='crash_id'
        )
        self.assertFalse(crash_store.transaction.called)
        config.logger.reset_mock()

        # test for normal save
        raw_crash = DotDict()
        raw_crash.submitted_timestamp = "2014-02-18 00:00:00+00"
        raw_crash.legacy_processing = 0
        crash_store.save_raw_crash(
            raw_crash=raw_crash,
            dumps=DotDict,
            crash_id='crash_id'
        )
        crash_store.transaction.assert_called_with(
            crash_store._save_raw_crash_transaction,
            raw_crash
        )
        crash_store.transaction.reset_mock()

        # test for save rejection because of "legacy_processing"
        raw_crash = DotDict()
        raw_crash.submitted_timestamp = "2014-02-18 00:00:00+00"
        raw_crash.legacy_processing = 5
        crash_store.save_raw_crash(
            raw_crash=raw_crash,
            dumps=DotDict,
            crash_id='crash_id'
        )
        self.assertFalse(crash_store.transaction.called)

    def test_save_raw_crash_no_legacy(self):
        config = self._setup_config()
        config.filter_on_legacy_processing = False
        crash_store = CassandraCrashStorage(config)

        raw_crash = DotDict(),
        # test for "legacy_processing" missing from crash
        crash_store.save_raw_crash(
            raw_crash,
            dumps=DotDict(),
            crash_id='crash_id'
        )
        crash_store.transaction.assert_called_with(
            crash_store._save_raw_crash_transaction,
            raw_crash
        )
        config.logger.reset_mock()

        # test for normal save
        raw_crash = DotDict()
        raw_crash.legacy_processing = 0
        crash_store.save_raw_crash(
            raw_crash=raw_crash,
            dumps=DotDict,
            crash_id='crash_id'
        )
        crash_store.transaction.assert_called_with(
            crash_store._save_raw_crash_transaction,
            raw_crash
        )
        crash_store.transaction.reset_mock()

        # test for save without regard to "legacy_processing" value
        raw_crash = DotDict()
        raw_crash.legacy_processing = 5
        crash_store.save_raw_crash(
            raw_crash=raw_crash,
            dumps=DotDict,
            crash_id='crash_id'
        )
        crash_store.transaction.assert_called_with(
            crash_store._save_raw_crash_transaction,
            raw_crash
        )

    def test_save_raw_crash_transaction_normal(self):
        connection = Mock()
        config = self._setup_config()
        crash_store = CassandraCrashStorage(config)
        raw_crash = DotDict()
        raw_crash.submitted_timestamp = datetime.datetime.strptime("2014-02-18 00:00:00",
            '%Y-%m-%d %H:%M:%S')
        raw_crash.legacy_processing = 5
        crash_store._save_raw_crash_transaction(connection, raw_crash)
