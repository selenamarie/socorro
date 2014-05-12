# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import mock
import json

from socorro.lib.util import SilentFakeLogger, DotDict
from socorro.external.crashstorage_base import Redactor
from socorro.external.ceph.crashstorage import CephCrashStorage
from socorro.database.transaction_executor import TransactionExecutor
from socorro.unittest.testbase import TestCase



class TestCrashStorage(TestCase):

    def _fake_processed_crash(self):
        d = DotDict()
        # these keys survive redaction
        d.a = DotDict()
        d.a.b = DotDict()
        d.a.b.c = 11
        d.sensitive = DotDict()
        d.sensitive.x = 2
        d.not_url = 'not a url'

        return d

    def _fake_redacted_processed_crash(self):
        d =  self._fake_unredacted_processed_crash()
        del d.url
        del d.email
        del d.user_id
        del d.exploitability
        del d.json_dump.sensitive
        del d.upload_file_minidump_flash1.json_dump.sensitive
        del d.upload_file_minidump_flash2.json_dump.sensitive
        del d.upload_file_minidump_browser.json_dump.sensitive

        return d

    def _fake_unredacted_processed_crash(self):
        d = self._fake_processed_crash()

        # these keys do not survive redaction
        d['url'] = 'http://very.embarassing.com'
        d['email'] = 'lars@fake.com'
        d['user_id'] = '3333'
        d['exploitability'] = 'yep'
        d.json_dump = DotDict()
        d.json_dump.sensitive = 22
        d.upload_file_minidump_flash1 = DotDict()
        d.upload_file_minidump_flash1.json_dump = DotDict()
        d.upload_file_minidump_flash1.json_dump.sensitive = 33
        d.upload_file_minidump_flash2 = DotDict()
        d.upload_file_minidump_flash2.json_dump = DotDict()
        d.upload_file_minidump_flash2.json_dump.sensitive = 33
        d.upload_file_minidump_browser = DotDict()
        d.upload_file_minidump_browser.json_dump = DotDict()
        d.upload_file_minidump_browser.json_dump.sensitive = DotDict()
        d.upload_file_minidump_browser.json_dump.sensitive.exploitable = 55
        d.upload_file_minidump_browser.json_dump.sensitive.secret = 66

        return d

    #def _fake_unredacted_processed_crash_as_string(self):
        #d = self._fake_unredacted_processed_crash()
        #s = json.dumps(d)
        #return s

    def setup_mocked_ceph_storage(self):
        config = DotDict({
            'source': {
                'dump_field': 'dump'
            },
            'transaction_executor_class': TransactionExecutor,
            'redactor_class': Redactor,
            'forbidden_keys': Redactor.required_config.forbidden_keys.default,
        })
        ceph = CephCrashStorage(config)
        ceph._encode = mock.Mock()
        ceph._request = mock.Mock()
        ceph._urlopen = mock.Mock()
        self._open_file = mock.Mock()
        return ceph

    def test_save_raw_crash(self):
        ceph_store = self.setup_mocked_ceph_storage()

        ceph_store.save_raw_crash({
            "submitted_timestamp": "2013-01-09T22:21:18.646733+00:00"
        }, {}, "0bba929f-8721-460c-dead-a43c20071027")
        #with ceph_store.hbase() as conn:
            #self.assertEqual(conn.table.call_count, 1)
            #self.assertEqual(conn.table.return_value.put.call_count, 1)

    def test_save_processed(self):
        ceph_store = self.setup_mocked_ceph_storage()
        ceph_store._encode.return_value =
        ceph_store.save_processed({
            "uuid": "936ce666-ff3b-4c7a-9674-367fe2120408",
            "completeddatetime": "2012-04-08 10:56:50.902884",
            "signature": 'now_this_is_a_signature'
        })
        #with ceph_store.hbase() as conn:
            #self.assertEqual(conn.table.call_count, 1)

    def test_get_raw_dumps(self):
        ceph_store = self.setup_mocked_ceph_storage()
        ceph_store.get_raw_dumps("936ce666-ff3b-4c7a-9674-367fe2120408")
        #with ceph_store.hbase() as conn:
            #self.assertEqual(conn.table.return_value.row.call_count, 1)

    def test_get_raw_dumps_as_files(self):
        ceph_store = self.setup_mocked_ceph_storage()
        ceph_store.get_raw_dumps_as_files(
            "936ce666-ff3b-4c7a-9674-367fe2120408")
        #with ceph_store.hbase() as conn:
            #self.assertEqual(conn.table.return_value.row.call_count, 1)

    def test_get_unredacted_processed(self):
        ceph_store = self.setup_mocked_ceph_storage()
        processed_crash = DotDict()
        #with ceph_store.hbase() as conn:
            #conn.table.return_value.row.return_value = {
                #'processed_data:json':
                #self._fake_unredacted_processed_crash_as_string()
            #}

            #processed_crash = ceph_store.get_unredacted_processed(
                #"936ce666-ff3b-4c7a-9674-367fe2120408"
            #)
            #self.assertEqual(
                #processed_crash,
                #self._fake_unredacted_processed_crash()
            #)

    def test_get_processed(self):
        ceph_store = self.setup_mocked_ceph_storage()
        faked_hb_row_object = DotDict()
        faked_hb_row_object.columns = DotDict()
        faked_hb_row_object.columns['processed_data:json'] = DotDict()
        faked_hb_row_object.columns['processed_data:json'].value = \
            self._fake_unredacted_processed_crash_as_string()

        processed_crash = DotDict()
        #with ceph_store.hbase() as conn:
            #conn.table.return_value.row.return_value = {
                #'processed_data:json':
                #self._fake_unredacted_processed_crash_as_string()
            #}

            #processed_crash = ceph_store.get_processed(
                #"936ce666-ff3b-4c7a-9674-367fe2120408"
            #)
            #self.assertEqual(
                #processed_crash,
                #self._fake_redacted_processed_crash()
            #)

    def test_get_processed_failure(self):
        ceph_store = self.setup_mocked_ceph_storage()
        #with ceph_store.hbase() as conn:
            #conn.table.return_value.row.return_value = {}
            #self.assertRaises(
                #CrashIDNotFound,
                #ceph_store.get_processed,
                #"936ce666-ff3b-4c7a-9674-367fe2120408"
            #)
