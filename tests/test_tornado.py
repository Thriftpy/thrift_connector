import contextlib
import random
import signal
import subprocess
import time

import os
import pytest
import tornado.testing

try:
    from mock import Mock
except ImportError:
    from unittest.mock import Mock

from datetime import timedelta
from tornado import gen
from tornado.concurrent import Future
from tornado.ioloop import IOLoop

from pingpong_app.fixtures import TestServerInfo


@contextlib.contextmanager
def fake_datetime(fake_time):
    def fake(*args):
        return fake_time

    ori_method = getattr(time, 'time')
    setattr(time, 'time', fake)
    try:
        yield
    finally:
        setattr(time, 'time', ori_method)


@pytest.fixture(scope='session')
def tornado_pingpong_thrift_server(request):
    port = random.randint(25536, 35536)
    port2 = random.randint(15536, 25536)
    config_path = "examples/gunicorn_config.py"
    gunicorn_server = subprocess.Popen(
        ["gunicorn_thrift", "examples.pingpong_app.app:app",
         "-c", config_path, "--bind", "0.0.0.0:%s" % port,
         "--bind", "0.0.0.0:%s" % port2, '--thrift-transport-factory', 'thriftpy.transport:TFramedTransportFactory']
    )

    def shutdown():
        os.kill(gunicorn_server.pid, signal.SIGTERM)

    request.addfinalizer(shutdown)
    time.sleep(4)

    return port, port2, gunicorn_server


@pytest.fixture(scope='class')
def tornado_pingpong_thrift_client(request, pingpong_service_key,
                                   pingpong_thrift_service, tornado_pingpong_thrift_server):
    port, port2, gunicorn_server = tornado_pingpong_thrift_server

    from thrift_connector.tornado import TornadoClientPool, TornadoThriftPyClient

    pool = TornadoClientPool(
        pingpong_thrift_service,
        'localhost',
        port,
        name=pingpong_service_key,
        connection_class=TornadoThriftPyClient
    )

    request.cls.pingpong_thrift_client = TestServerInfo(
        'localhost',
        port,
        gunicorn_server,
        pool,
        pingpong_thrift_service,
        port2=port2
    )


class _AsyncTestCase(tornado.testing.AsyncTestCase):
    def get_new_ioloop(self):
        return IOLoop.instance()


@pytest.mark.usefixtures('tornado_pingpong_thrift_client')
class ClientPoolTest(_AsyncTestCase):
    def _cleanup_pool(self):
        pool = self.pingpong_thrift_client.pool
        for c in pool.connections:
            c.close()
        pool.connections = pool.QueueCls()

    def setUp(self):
        super(ClientPoolTest, self).setUp()
        self._cleanup_pool()

    def tearDown(self):
        super(ClientPoolTest, self).tearDown()
        self._cleanup_pool()

    @tornado.testing.gen_test
    def test_client_pool(self):
        old_client = None

        with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
            yield c.ping()
            old_client = c

        with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
            yield c.ping()
            self.assertIs(c, old_client)

    @tornado.testing.gen_test
    def test_client_pool_dead_connection_occured(self):
        os.kill(self.pingpong_thrift_client.process.pid, signal.SIGHUP)  # restart
        time.sleep(1)

        with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
            yield c.ping()
            old_client = c

        with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
            yield c.ping()
            self.assertIs(c, old_client)

    @tornado.testing.gen_test
    def test_client_pool_disabled(self):
        pool = self.pingpong_thrift_client.pool
        pool.max_conn = 0

        with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
            yield c.ping()
            old_client = c

        with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
            yield c.ping()
            self.assertIsNot(c, old_client)

    @tornado.testing.gen_test
    def test_client_pool_overflow(self):
        from thriftpy.transport import TTransportException
        from thrift_connector.tornado import TornadoClientPool

        pool = self.pingpong_thrift_client.pool
        pool.max_conn = 3

        ori_method = getattr(TornadoClientPool, 'get_client_from_pool')

        def mock_method(*args):
            future = Future()
            future.set_result(None)
            return future

        setattr(TornadoClientPool, 'get_client_from_pool', mock_method)
        try:
            available_clients = []
            for _ in range(5):
                with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
                    yield c.ping()
                    available_clients.append(c)

            pool = self.pingpong_thrift_client.pool
            self.assertLessEqual(len(pool.connections), pool.max_conn)

            for n, c in enumerate(available_clients[:pool.max_conn]):
                if n > pool.max_conn - 1:
                    # Overflown connections should be closed after use.
                    try:
                        yield c.ping()
                    except TTransportException:
                        pass
                    else:
                        self.fail('Overflown connections should be closed after use')
                else:
                    yield c.ping()
        finally:
            setattr(TornadoClientPool, 'get_client_from_pool', ori_method)

    @tornado.testing.gen_test
    def test_client_call_and_put_back(self):
        pool = self.pingpong_thrift_client.pool

        yield pool.ping()
        yield pool.ping()
        yield pool.ping()

        self.assertEqual(len(pool.connections), 1)
        conn = list(pool.connections)[0]

        for c in pool.connections:
            c.close()

        yield pool.ping()
        yield pool.ping()
        yield pool.ping()

        self.assertEqual(len(pool.connections), 1)
        self.assertIsNot(conn, list(pool.connections)[0])

    @tornado.testing.gen_test
    def test_ttronsport_exception_not_put_back(self):
        pool = self.pingpong_thrift_client.pool

        with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
            yield c.ping()

        self.assertEqual(len(pool.connections), 1)

        # If TTransportException occurs, conn shouldn't be put back into pool.
        from thriftpy.transport import TTransportException
        try:
            with (yield self.pingpong_thrift_client.pool.connection_ctx()):
                raise TTransportException
        except TTransportException:
            pass
        else:
            self.fail('TTransportException should be re-raised')

        self.assertEqual(len(pool.connections), 0)

        with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
            yield c.ping()
            old_client = c

        self.assertEqual(len(pool.connections), 1)

        # If predefined exception occurs, conn should be put back and available.
        try:
            with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
                raise self.pingpong_thrift_client.service.AboutToShutDownException
        except self.pingpong_thrift_client.service.AboutToShutDownException:
            pass
        else:
            self.fail('Predefined exception should be raised')

        with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
            self.assertIs(c, old_client)
            yield c.ping()

    @tornado.testing.gen_test
    def test_should_not_put_back_connection_if_ttransport_exception_raised(self):
        pool = self.pingpong_thrift_client.pool

        with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
            yield c.ping()

        self.assertEqual(len(pool.connections), 1)

        def should_fail_api():
            future = Future()
            future.set_exception(c.TTransportException())
            return future

        c.should_fail_api = should_fail_api

        # If TTransportException occurs, conn shouldn't be put back into pool.
        try:
            yield pool.should_fail_api()
        except c.TTransportException:
            pass
        else:
            self.fail('TTransportException should be re-raised')

        self.assertEqual(len(pool.connections), 0)

        with (yield self.pingpong_thrift_client.pool.connection_ctx()) as c:
            yield c.ping()

        self.assertEqual(len(pool.connections), 1)

        def should_fail_api():
            future = Future()
            future.set_exception(self.pingpong_thrift_client.service.AboutToShutDownException())
            return future

        c.should_fail_api = should_fail_api

        # If predefined exception occurs, conn should be put back and available.
        try:
            yield pool.should_fail_api()
        except self.pingpong_thrift_client.service.AboutToShutDownException:
            pass
        else:
            self.fail('Predefined exception should be raised')

        self.assertEqual(len(pool.connections), 1)
        self.assertEqual(list(pool.connections)[0], c)
        yield pool.ping()

    @tornado.testing.gen_test
    def test_setted_connection_pool_connection_keepalive(self):
        keep_alive = 1
        from thrift_connector.tornado import TornadoClientPool
        pool = TornadoClientPool(
            self.pingpong_thrift_client.pool.service,
            self.pingpong_thrift_client.host,
            self.pingpong_thrift_client.port,
            name=self.pingpong_thrift_client.pool.name,
            raise_empty=False, max_conn=3,
            connection_class=self.pingpong_thrift_client.pool.connection_class,
            keepalive=keep_alive
        )
        self.assertEqual(pool.keepalive, keep_alive)
        with (yield pool.connection_ctx()) as conn:
            now = time.time()
            self.assertEqual(int(conn.alive_until), int(now + keep_alive))
            self.assertTrue((yield conn.test_connection()))
            old_connection = conn

        with fake_datetime(now + 0.1):
            with (yield pool.connection_ctx()) as conn:
                self.assertIs(conn, old_connection)

        with fake_datetime(now + keep_alive + 2):
            self.assertFalse((yield old_connection.test_connection()))

            with (yield pool.connection_ctx()) as conn:
                self.assertIsNot(old_connection, conn)

    @tornado.testing.gen_test
    def test_not_setted_connection_pool_connection_keepalive(self):
        from thrift_connector.tornado import TornadoClientPool
        pool = TornadoClientPool(
            self.pingpong_thrift_client.pool.service,
            self.pingpong_thrift_client.host,
            self.pingpong_thrift_client.port,
            name=self.pingpong_thrift_client.pool.name,
            raise_empty=False, max_conn=3,
            connection_class=self.pingpong_thrift_client.pool.connection_class
        )
        self.assertIs(pool.keepalive, None)
        with (yield pool.connection_ctx()) as conn:
            now = time.time()
            self.assertIs(conn.alive_until, None)
            self.assertTrue((yield conn.test_connection()))
            old_connection = conn

        with fake_datetime(now + 0.1):
            with (yield pool.connection_ctx()) as conn:
                self.assertIs(conn, old_connection)

        with fake_datetime(now + timedelta(days=100).seconds):
            self.assertTrue((yield old_connection.test_connection()))

            with (yield pool.connection_ctx()) as conn:
                self.assertIs(old_connection, conn)


@pytest.mark.usefixtures('tornado_pingpong_thrift_client')
class ClientPoolGenerationTest(_AsyncTestCase):
    @tornado.testing.gen_test
    def test_connection_pool_generation(self):
        pool = self.pingpong_thrift_client.pool
        c = pool.produce_client()
        self.assertEqual(c.pool_generation, pool.generation)
        self.assertEqual(c.pool_generation, 0)

        pool.clear()

        c2 = pool.produce_client()
        self.assertEqual(c2.pool_generation, pool.generation)
        self.assertEqual(c2.pool_generation, 1)

        pool.put_back_connection(c)
        pool.put_back_connection(c2)

        for c in pool.connections:
            self.assertEqual(c.pool_generation, pool.generation)


@pytest.mark.usefixtures('tornado_pingpong_thrift_client')
class RandomMultiServerClientTest(_AsyncTestCase):
    @tornado.testing.gen_test
    def test_random_multiconnection_pool(self):
        servers = [
            (self.pingpong_thrift_client.host, self.pingpong_thrift_client.port),
            (self.pingpong_thrift_client.host, self.pingpong_thrift_client.port2),
        ]

        from thrift_connector.tornado import TornadoRandomMultiServerClient
        random_pool = TornadoRandomMultiServerClient(
            self.pingpong_thrift_client.pool.service,
            servers=servers,
            name=self.pingpong_thrift_client.pool.name,
            raise_empty=False, max_conn=3,
            connection_class=self.pingpong_thrift_client.pool.connection_class,
        )

        with (yield random_pool.connection_ctx()) as conn:
            self.assertTrue((yield conn.test_connection()))


@pytest.mark.usefixtures('tornado_pingpong_thrift_client')
class RoundRobinMultiServerClientTest(_AsyncTestCase):
    @tornado.testing.gen_test
    def test_roundrobin_multiconnection_pool(self):
        servers = [
            (self.pingpong_thrift_client.host, self.pingpong_thrift_client.port),
            (self.pingpong_thrift_client.host, self.pingpong_thrift_client.port2),
        ]

        from thrift_connector.tornado import TornadoRoundRobinMultiServerClient
        roundrobin_pool = TornadoRoundRobinMultiServerClient(
            self.pingpong_thrift_client.pool.service,
            servers=servers,
            name=self.pingpong_thrift_client.pool.name,
            raise_empty=False, max_conn=3,
            connection_class=self.pingpong_thrift_client.pool.connection_class,
        )

        conn1 = roundrobin_pool.produce_client()
        self.assertTrue((yield conn1.test_connection()))

        conn2 = roundrobin_pool.produce_client()
        self.assertTrue((yield conn2.test_connection()))
        self.assertNotEqual((conn1.host, conn1.port), (conn2.host, conn2.port))

        conn3 = roundrobin_pool.produce_client()
        self.assertEqual((conn1.host, conn1.port), (conn3.host, conn3.port))
        self.assertNotEqual((conn2.host, conn2.port), (conn3.host, conn3.port))

        conn4 = roundrobin_pool.produce_client()
        self.assertNotEqual((conn1.host, conn1.port), (conn4.host, conn4.port))
        self.assertEqual((conn2.host, conn2.port), (conn4.host, conn4.port))


@pytest.mark.usefixtures('tornado_pingpong_thrift_client')
class HeartbeatClientPoolTest(_AsyncTestCase):
    @tornado.testing.gen_test(timeout=10)
    def test_heartbeat_client_pool(self):
        from thrift_connector.tornado import TornadoHeartbeatClientPool
        heartbeat_pool = TornadoHeartbeatClientPool(
            self.pingpong_thrift_client.pool.service,
            self.pingpong_thrift_client.host,
            self.pingpong_thrift_client.port,
            name=self.pingpong_thrift_client.pool.name,
            timeout=1,
            connection_class=self.pingpong_thrift_client.pool.connection_class,
            max_conn=3,
            check_interval=2,
        )

        conn1 = yield heartbeat_pool.get_client()
        self.assertTrue((yield conn1.test_connection()))

        # now we kill client and put back to pool
        conn1.close()
        heartbeat_pool.put_back_connection(conn1)
        self.assertEqual(heartbeat_pool.pool_size(), 1)

        # this call should fail
        disconnected_client = yield heartbeat_pool.get_client()
        self.assertFalse((yield disconnected_client.test_connection()))
        self.assertTrue(heartbeat_pool.put_back_connection(disconnected_client))
        self.assertEqual(heartbeat_pool.pool_size(), 1)

        yield gen.sleep(1)
        self.assertEqual(heartbeat_pool.pool_size(), 1)
        # after check_interval, connection need check, but connection is dead
        yield gen.sleep(2)
        # disconnection should be detected and dead clients removed (1 client may not be counted if it is being checked)
        self.assertEqual(heartbeat_pool.pool_size(), 0)

        for _ in range(3):
            conn = heartbeat_pool.produce_client()
            heartbeat_pool.put_back_connection(conn)

        yield gen.sleep(4)

        # Make sure all clients have been checked
        use_counts = [client.use_count for client in heartbeat_pool.connections]
        self.assertTrue(all(use_counts))


@pytest.mark.usefixtures('tornado_pingpong_thrift_client')
class APICallContextTest(_AsyncTestCase):
    @tornado.testing.gen_test
    def test_api_call_context(self):
        from thrift_connector.hooks import before_call, after_call

        pool = self.pingpong_thrift_client.pool

        mock_before_hook = Mock()
        mock_after_hook = Mock()
        before_call.register(mock_before_hook)
        after_call.register(mock_after_hook)

        fake_time = time.time()
        with fake_datetime(fake_time):
            with (yield pool.connection_ctx()) as conn:
                ping_future = conn.ping()
                yield ping_future

            mock_before_hook.assert_called_with(pool, conn, 'ping', fake_time)
            mock_after_hook.assert_called_with(pool, conn, 'ping', fake_time, 0, ping_future)

        # raise Exception when raises specified
        mock_before_hook_with_err = Mock(side_effect=TypeError('test'))
        before_call.register(mock_before_hook_with_err, raises=(TypeError,))
        with (yield pool.connection_ctx()) as client:
            with pytest.raises(TypeError) as exc_info:
                yield client.win()

        assert "test" in str(exc_info.value)
        before_call.callbacks.clear()
        after_call.callbacks.clear()


@pytest.mark.usefixtures('tornado_pingpong_thrift_client')
class ConnCloseHookTest(_AsyncTestCase):
    @tornado.testing.gen_test
    def test_conn_close_hook(self):
        pool = self.pingpong_thrift_client.pool
        close_mock = Mock()
        pool.register_after_close_func(close_mock)
        client = yield pool.get_client()
        client.close()
        close_mock.assert_called_with(pool, client)


@pytest.mark.usefixtures('tornado_pingpong_thrift_client')
class SetTimeoutTest(_AsyncTestCase):
    @tornado.testing.gen_test
    def test_set_timeout(self):
        pool = self.pingpong_thrift_client.pool
        client = yield pool.get_client()

        client.set_client_timeout(0.5 * 1000)
        self.assertEqual((yield client.sleep(0.2)), 'good morning')

        with pytest.raises(client.TTransportException) as e:
            yield client.sleep(1)

        self.assertIn('type=3', str(e.value))
        client.close()

        with pytest.raises(client.TTransportException) as e:
            with (yield pool.connection_ctx(timeout=1)) as client:
                yield client.sleep(2)
        self.assertIn('type=3', str(e.value))


@pytest.mark.usefixtures('tornado_pingpong_thrift_client')
class FillConnectionPoolTest(_AsyncTestCase):
    @tornado.testing.gen_test
    def test_fill_conneciont_pool(self):
        from thrift_connector.tornado import TornadoBaseClientPool
        pool = TornadoBaseClientPool(
            self.pingpong_thrift_client.pool.service,
            connection_class=self.pingpong_thrift_client.pool.connection_class
        )
        self.assertEqual(pool.max_conn, 30)
        self.assertEqual(pool.pool_size(), 0)
        pool.yield_server = Mock(
            return_value=(
                self.pingpong_thrift_client.host, self.pingpong_thrift_client.port))
        pool.fill_connection_pool()
        self.assertEqual(pool.pool_size(), pool.max_conn)
