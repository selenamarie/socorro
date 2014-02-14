# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import unittest

from mock import (
    Mock,
    call,
    patch
)
from threading import currentThread

from socorro.external.cassandra.connection_context import (
    Connection,
    ConnectionContext,
    ConnectionContextPooled
)
from socorro.lib.util import DotDict


#==============================================================================
class TestConnection(unittest.TestCase):
    """Test Cassandra class. """

    #--------------------------------------------------------------------------
    def test_constructor(self):
        faked_connection_object = Mock()
        config = DotDict()
        conn = Connection(
            config,
            faked_connection_object
        )
        self.assertTrue(conn.config is config)
        self.assertTrue(conn.connection is faked_connection_object)


    #--------------------------------------------------------------------------
    def test_close(self):
        faked_connection_object = Mock()
        config = DotDict()
        conn = Connection(
            config,
            faked_connection_object
        )
        conn.close()
        faked_connection_object.close.assert_called_once_with()



#==============================================================================
class TestConnectionContext(unittest.TestCase):

    #--------------------------------------------------------------------------
    def _setup_config(self):
        config = DotDict();
        config.host = 'localhost'
        config.virtual_host = '/'
        config.port = '5672'
        config.cassandra_user = 'guest'
        config.cassandra_password = 'guest'
        config.keyspace = 'OtherData'
        config.timeout = 10
        config.pool_size = 3
        config.cassandra_connection_wrapper_class = Connection

        config.executor_identity = lambda: 'MainThread'

        return config

    #--------------------------------------------------------------------------
    def test_constructor(self):
        conn_context_functor = ConnectionContext(self._setup_config)
        self.assertTrue(
            conn_context_functor.config is conn_context_functor.local_config
        )

    #--------------------------------------------------------------------------
    def test_connection(self):
        config = self._setup_config()
        pycassa_string = 'socorro.external.cassandra.connection_context.pycassa'
        with patch(pycassa_string) as mocked_pycassa_module:
            conn_context_functor = ConnectionContext(config)
            conn = conn_context_functor.connection()
            mocked_pycassa_module.pool.ConnectionPool.assert_called_once_with(
                conn_context_functor.config.keyspace,
                server_list=[conn_context_functor.config.host + ':' +
                    conn_context_functor.config.port],
                credentials={'username': conn_context_functor.config.cassandra_user,
                    'password': conn_context_functor.config.cassandra_password},
                timeout=conn_context_functor.config.timeout,
                pool_size=conn_context_functor.config.pool_size
            )
            mocked_pycassa_module.BlockingConnection.assert_called_one_with(
                mocked_pycassa_module.ConnectionParameters.return_value
            )
            self.assertTrue(isinstance(conn, Connection))
            self.assertTrue(conn.config is config)
            self.assertTrue(
                conn.connection is
                    mocked_pycassa_module.pool.ConnectionPool.return_value
            )

    #--------------------------------------------------------------------------
    def test_call_and_close_connecton(self):
        config = self._setup_config()
        pycassa_string = 'socorro.external.cassandra.connection_context.pycassa'
        with patch(pycassa_string) as mocked_pycassa_module:
            conn_context_functor = ConnectionContext(config)
            with conn_context_functor() as conn_context:
                self.assertTrue(isinstance(conn_context, Connection))
            conn_context.connection.close.assert_called_once_with()

#==============================================================================
class TestConnectionContextPooled(unittest.TestCase):

    #--------------------------------------------------------------------------
    def _setup_config(self):
        config = DotDict();
        config.host = 'localhost'
        config.port = '9160'
        config.cassandra_user = ''
        config.cassandra_password = ''
        config.keyspace = 'OtherData'
        config.timeout = 10
        config.pool_size = 3
        config.logger = Mock()
        config.cassandra_connection_wrapper_class = Connection

        config.executor_identity = lambda: 'MainThread'

        return config

    #--------------------------------------------------------------------------
    def test_constructor(self):
        conn_context_functor = ConnectionContextPooled(self._setup_config)
        self.assertTrue(
            conn_context_functor.config is conn_context_functor.local_config
        )
        self.assertEqual(len(conn_context_functor.pool), 0)

    #--------------------------------------------------------------------------
    def test_connection(self):
        config = self._setup_config()
        pycassa_string = 'socorro.external.cassandra.connection_context.pycassa'
        with patch(pycassa_string) as mocked_pycassa_module:
            conn_context_functor = ConnectionContextPooled(config)
            conn = conn_context_functor.connection()
            self.assertTrue(
                conn is conn_context_functor.pool[currentThread().getName()]
            )
            conn = conn_context_functor.connection('dwight')
            self.assertTrue(
                conn is conn_context_functor.pool['dwight']
            )
            # get the same connection again to make sure it really is the same
            conn = conn_context_functor.connection()
            self.assertTrue(
                conn is conn_context_functor.pool[currentThread().getName()]
            )

    #--------------------------------------------------------------------------
    def test_close_connection(self):
        config = self._setup_config()
        pycassa_string = 'socorro.external.cassandra.connection_context.pycassa'
        with patch(pycassa_string) as mocked_pycassa_module:
            conn_context_functor = ConnectionContextPooled(config)
            conn = conn_context_functor.connection('dwight')
            self.assertTrue(
                conn is conn_context_functor.pool['dwight']
            )
            conn_context_functor.close_connection(conn)
            # should be no change
            self.assertTrue(
                conn is conn_context_functor.pool['dwight']
            )
            self.assertEqual(len(conn_context_functor.pool), 1)

            conn_context_functor.close_connection(conn, True)
            self.assertRaises(
                KeyError,
                lambda : conn_context_functor.pool['dwight']
            )
            self.assertEqual(len(conn_context_functor.pool), 0)


    #--------------------------------------------------------------------------
    def test_close(self):
        config = self._setup_config()
        pycassa_string = 'socorro.external.cassandra.connection_context.pycassa'
        with patch(pycassa_string) as mocked_pycassa_module:
            conn_context_functor = ConnectionContextPooled(config)
            conn = conn_context_functor.connection()
            conn = conn_context_functor.connection('dwight')
            conn = conn_context_functor.connection('wilma')
            conn_context_functor.close()
            self.assertEqual(len(conn_context_functor.pool), 0)

    #--------------------------------------------------------------------------
    def test_force_reconnect(self):
        config = self._setup_config()
        pycassa_string = 'socorro.external.cassandra.connection_context.pycassa'
        with patch(pycassa_string) as mocked_pycassa_module:
            conn_context_functor = ConnectionContextPooled(config)
            conn = conn_context_functor.connection()
            self.assertTrue(
                conn is conn_context_functor.pool[currentThread().getName()]
            )
            conn_context_functor.force_reconnect()
            self.assertEqual(len(conn_context_functor.pool), 0)
            conn2 = conn_context_functor.connection()
            self.assertFalse(conn == conn2)

