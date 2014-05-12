# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import mock
import json
import datetime

from socorro.lib.util import SilentFakeLogger, DotDict
from socorro.external.crashstorage_base import Redactor
from socorro.external.ceph.crashstorage import CephCrashStorage
from socorro.database.transaction_executor import TransactionExecutor
import socorro.unittest.testbase

a_raw_crash = {
    "submitted_timestamp": "2013-01-09T22:21:18.646733+00:00"
}
a_raw_crash_as_string = json.dumps(a_raw_crash)

class TestCase(socorro.unittest.testbase.TestCase):

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
            'logger': mock.Mock(),
            'host': 'ceph.is.out.here.somewhere',
            'access_key': 'this is the access key',
            'secret_access_key': 'secrets',
            'buckets': 'daily',
        })
        ceph = CephCrashStorage(config)
        ceph._connect_to_ceph = mock.Mock()
        ceph._mocked_connection = ceph._connect_to_ceph.return_value
        ceph._calling_format = mock.Mock()
        ceph._calling_format.return_value = mock.Mock()
        ceph._CreateError = mock.Mock()
        ceph._open = mock.MagicMock()
        return ceph

    def test_save_raw_crash_1(self):
        ceph_store = self.setup_mocked_ceph_storage()

        # the tested call
        ceph_store.save_raw_crash(
            {"submitted_timestamp": "2013-01-09T22:21:18.646733+00:00"},
            {},
            "0bba929f-8721-460c-dead-a43c20071027"
        )

        # what should have happened internally
        self.assertEqual(ceph_store._calling_format.call_count, 2)
        ceph_store._calling_format.assert_called_with()

        self.assertEqual(ceph_store._connect_to_ceph.call_count, 2)
        ceph_store._connect_to_ceph.assert_called_with(
            aws_access_key_id=ceph_store.config.access_key,
            aws_secret_access_key=ceph_store.config.secret_access_key,
            host=ceph_store.config.host,
            is_secure=False,
            calling_format=ceph_store._calling_format.return_value
        )

        self.assertEqual(
            ceph_store._mocked_connection.create_bucket.call_count,
            2
        )
        ceph_store._mocked_connection.create_bucket.assert_called_with(
            '071027'
        )

        bucket_mock = ceph_store._mocked_connection.create_bucket.return_value
        self.assertEqual(bucket_mock.new_key.call_count, 2)
        bucket_mock.new_key.assert_has_calls(
            [
                mock.call('0bba929f-8721-460c-dead-a43c20071027.raw_crash'),
                mock.call('0bba929f-8721-460c-dead-a43c20071027.dump_names'),
            ],
            any_order=True,
        )

        storage_key_mock = bucket_mock.new_key.return_value
        self.assertEqual(
            storage_key_mock.set_contents_from_string.call_count,
            2
        )
        storage_key_mock.set_contents_from_string.assert_has_calls(
            [
                mock.call(
                    '{"submitted_timestamp": '
                    '"2013-01-09T22:21:18.646733+00:00"}'
                ),
                mock.call('[]'),
            ],
            any_order=True,
        )

    def test_save_raw_crash_2(self):
        ceph_store = self.setup_mocked_ceph_storage()

        # the tested call
        ceph_store.save_raw_crash(
            {"submitted_timestamp": "2013-01-09T22:21:18.646733+00:00"},
            {'dump': 'fake dump', 'flash_dump': 'fake flash dump'},
            "0bba929f-8721-460c-dead-a43c20071027"
        )

        # what should have happened internally
        self.assertEqual(ceph_store._calling_format.call_count, 4)
        ceph_store._calling_format.assert_called_with()

        self.assertEqual(ceph_store._connect_to_ceph.call_count, 4)
        ceph_store._connect_to_ceph.assert_called_with(
            aws_access_key_id=ceph_store.config.access_key,
            aws_secret_access_key=ceph_store.config.secret_access_key,
            host=ceph_store.config.host,
            is_secure=False,
            calling_format=ceph_store._calling_format.return_value
        )

        self.assertEqual(
            ceph_store._mocked_connection.create_bucket.call_count,
            4
        )
        ceph_store._mocked_connection.create_bucket.assert_called_with(
            '071027'
        )

        bucket_mock = ceph_store._mocked_connection.create_bucket.return_value
        self.assertEqual(bucket_mock.new_key.call_count, 4)
        bucket_mock.new_key.assert_has_calls(
            [
                mock.call('0bba929f-8721-460c-dead-a43c20071027.raw_crash'),
                mock.call('0bba929f-8721-460c-dead-a43c20071027.dump_names'),
                mock.call('0bba929f-8721-460c-dead-a43c20071027.dump'),
                mock.call('0bba929f-8721-460c-dead-a43c20071027.flash_dump'),
            ],
            any_order=True,
        )

        storage_key_mock = bucket_mock.new_key.return_value
        self.assertEqual(
            storage_key_mock.set_contents_from_string.call_count,
            4
        )
        print storage_key_mock.set_contents_from_string.call_args_list
        storage_key_mock.set_contents_from_string.assert_has_calls(
            [
                mock.call(
                    '{"submitted_timestamp": '
                    '"2013-01-09T22:21:18.646733+00:00"}'
                ),
                mock.call('["flash_dump", "dump"]'),
                mock.call('fake dump'),
                mock.call('fake flash dump'),
            ],
            any_order=True,
        )

    def test_save_processed(self):
        ceph_store = self.setup_mocked_ceph_storage()

        # the tested call
        ceph_store.save_processed({
            "uuid": "0bba929f-8721-460c-dead-a43c20071027",
            "completeddatetime": "2012-04-08 10:56:50.902884",
            "signature": 'now_this_is_a_signature'
        })

        # what should have happened internally
        self.assertEqual(ceph_store._calling_format.call_count, 1)
        ceph_store._calling_format.assert_called_with()

        self.assertEqual(ceph_store._connect_to_ceph.call_count, 1)
        ceph_store._connect_to_ceph.assert_called_with(
            aws_access_key_id=ceph_store.config.access_key,
            aws_secret_access_key=ceph_store.config.secret_access_key,
            host=ceph_store.config.host,
            is_secure=False,
            calling_format=ceph_store._calling_format.return_value
        )

        self.assertEqual(
            ceph_store._mocked_connection.create_bucket.call_count,
            1
        )
        ceph_store._mocked_connection.create_bucket.assert_called_with(
            '071027'
        )

        bucket_mock = ceph_store._mocked_connection.create_bucket.return_value
        self.assertEqual(bucket_mock.new_key.call_count, 1)
        bucket_mock.new_key.assert_has_calls(
            [
                mock.call(
                    '0bba929f-8721-460c-dead-a43c20071027.processed_crash'
                ),
            ],
        )

        storage_key_mock = bucket_mock.new_key.return_value
        self.assertEqual(
            storage_key_mock.set_contents_from_string.call_count,
            1
        )
        storage_key_mock.set_contents_from_string.assert_has_calls(
            [
                mock.call(
                    '{"signature": "now_this_is_a_signature", "uuid": '
                    '"0bba929f-8721-460c-dead-a43c20071027", "completed'
                    'datetime": "2012-04-08 10:56:50.902884"}'
                ),
            ],
            any_order=True,
        )

    def test_get_craw_crash(self):
        # setup some internal behaviors and fake outs
        ceph_store = self.setup_mocked_ceph_storage()
        mocked_get_contents_as_string = (
            ceph_store._connect_to_ceph.return_value
            .create_bucket.return_value
            .get_contents_as_string
        )
        mocked_get_contents_as_string.side_effect = [
            a_raw_crash_as_string
        ]

        # the tested call
        result = ceph_store.get_raw_crash(
            "936ce666-ff3b-4c7a-9674-367fe2120408"
        )

        # what should have happened internally
        self.assertEqual(ceph_store._calling_format.call_count, 1)
        ceph_store._calling_format.assert_called_with()

        self.assertEqual(ceph_store._connect_to_ceph.call_count, 1)
        ceph_store._connect_to_ceph.assert_called_with(
            aws_access_key_id=ceph_store.config.access_key,
            aws_secret_access_key=ceph_store.config.secret_access_key,
            host=ceph_store.config.host,
            is_secure=False,
            calling_format=ceph_store._calling_format.return_value
        )

        self.assertEqual(
            ceph_store._mocked_connection.create_bucket.call_count,
            1
        )
        ceph_store._mocked_connection.create_bucket.assert_called_with(
            '120408'
        )

        bucket_mock = ceph_store._mocked_connection.create_bucket.return_value
        self.assertEqual(bucket_mock.get_contents_as_string.call_count, 1)
        bucket_mock.get_contents_as_string.assert_has_calls(
            [
                mock.call(
                    '936ce666-ff3b-4c7a-9674-367fe2120408.raw_crash'
                ),
            ],
        )

        self.assertEqual(result, a_raw_crash)

    def test_get_raw_dump(self):
        # setup some internal behaviors and fake outs
        ceph_store = self.setup_mocked_ceph_storage()
        mocked_get_contents_as_string = (
            ceph_store._connect_to_ceph.return_value
            .create_bucket.return_value
            .get_contents_as_string
        )
        mocked_get_contents_as_string.side_effect = [
            'this is a raw dump'
        ]

        # the tested call
        result = ceph_store.get_raw_dump(
            "936ce666-ff3b-4c7a-9674-367fe2120408"
        )

        # what should have happened internally
        self.assertEqual(ceph_store._calling_format.call_count, 1)
        ceph_store._calling_format.assert_called_with()

        self.assertEqual(ceph_store._connect_to_ceph.call_count, 1)
        ceph_store._connect_to_ceph.assert_called_with(
            aws_access_key_id=ceph_store.config.access_key,
            aws_secret_access_key=ceph_store.config.secret_access_key,
            host=ceph_store.config.host,
            is_secure=False,
            calling_format=ceph_store._calling_format.return_value
        )

        self.assertEqual(
            ceph_store._mocked_connection.create_bucket.call_count,
            1
        )
        ceph_store._mocked_connection.create_bucket.assert_called_with(
            '120408'
        )

        bucket_mock = ceph_store._mocked_connection.create_bucket.return_value
        self.assertEqual(bucket_mock.get_contents_as_string.call_count, 1)
        bucket_mock.get_contents_as_string.assert_has_calls(
            [
                mock.call(
                    '936ce666-ff3b-4c7a-9674-367fe2120408.dump'
                ),
            ],
        )

        self.assertEqual(result, 'this is a raw dump')

    def test_get_raw_dumps(self):
        # setup some internal behaviors and fake outs
        ceph_store = self.setup_mocked_ceph_storage()
        mocked_get_contents_as_string = (
            ceph_store._connect_to_ceph.return_value
            .create_bucket.return_value
            .get_contents_as_string
        )
        mocked_get_contents_as_string.side_effect = [
            '["dump", "flash_dump", "city_dump"]',
            'this is "dump", the first one',
            'this is "flash_dump", the second one',
            'this is "city_dump", the last one',
        ]

        # the tested call
        result = ceph_store.get_raw_dumps(
            "936ce666-ff3b-4c7a-9674-367fe2120408"
        )

        # what should have happened internally
        self.assertEqual(ceph_store._calling_format.call_count, 4)
        ceph_store._calling_format.assert_called_with()

        self.assertEqual(ceph_store._connect_to_ceph.call_count, 4)
        ceph_store._connect_to_ceph.assert_called_with(
            aws_access_key_id=ceph_store.config.access_key,
            aws_secret_access_key=ceph_store.config.secret_access_key,
            host=ceph_store.config.host,
            is_secure=False,
            calling_format=ceph_store._calling_format.return_value
        )

        self.assertEqual(
            ceph_store._mocked_connection.create_bucket.call_count,
            4
        )
        ceph_store._mocked_connection.create_bucket.assert_called_with(
            '120408'
        )

        bucket_mock = ceph_store._mocked_connection.create_bucket.return_value
        self.assertEqual(bucket_mock.get_contents_as_string.call_count, 4)
        bucket_mock.get_contents_as_string.assert_has_calls(
            [
                mock.call(
                    '936ce666-ff3b-4c7a-9674-367fe2120408.dump_names'
                ),
                mock.call(
                    '936ce666-ff3b-4c7a-9674-367fe2120408.dump'
                ),
                mock.call(
                    '936ce666-ff3b-4c7a-9674-367fe2120408.flash_dump'
                ),
                mock.call(
                    '936ce666-ff3b-4c7a-9674-367fe2120408.city_dump'
                ),
            ],
        )

        self.assertEqual(
            result,
            {
                "dump": 'this is "dump", the first one',
                "flash_dump": 'this is "flash_dump", the second one',
                "city_dump": 'this is "city_dump", the last one',
            }
        )

    def test_get_raw_dumps_as_files(self):
        ceph_store = self.setup_mocked_ceph_storage()
        ceph_store.open.return_value = mock.MagicMock()
        ceph_store.get_raw_dumps_as_files(
            "936ce666-ff3b-4c7a-9674-367fe2120408")

    #def test_get_unredacted_processed(self):
        #ceph_store = self.setup_mocked_ceph_storage()
        #processed_crash = DotDict()

    #def test_get_processed(self):
        #ceph_store = self.setup_mocked_ceph_storage()
        #faked_hb_row_object = DotDict()
        #faked_hb_row_object.columns = DotDict()
        #faked_hb_row_object.columns['processed_data:json'] = DotDict()
        #faked_hb_row_object.columns['processed_data:json'].value = \
            #self._fake_unredacted_processed_crash_as_string()

        #processed_crash = DotDict()

    #def test_get_processed_failure(self):
        #ceph_store = self.setup_mocked_ceph_storage()
