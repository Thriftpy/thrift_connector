# -*- coding: utf-8 -*-

#  from gevent import monkey; monkey.patch_all()
import os
import time
import signal
import socket
from datetime import timedelta

import pytest
try:
    from mock import Mock
except ImportError:
    from unittest.mock import Mock

from thrift_connector.connection_pool import (
    ClientPool,
    BaseClientPool,
    RoundRobinMultiServerClient,
    RandomMultiServerClient,
    HeartbeatClientPool,
)
from thriftpy.transport import TTransportException


@pytest.fixture
def fake_datetime(monkeypatch):
    class mock_datetime(object):
        FAKE_TIME = 1412784000.0

        @classmethod
        def time(cls):
            return cls.FAKE_TIME
    monkeypatch.setattr(time, 'time', mock_datetime.time)
    return mock_datetime


@pytest.fixture
def fake_time(monkeypatch):
    class mock_time(object):
        FAKE_TIME = 1431587880.601526

        @classmethod
        def time(cls):
            return cls.FAKE_TIME
    monkeypatch.setattr(time, 'time', mock_time.time)
    return mock_time


@pytest.fixture
def init_pingpong_pool(request, pingpong_service_key, pingpong_thrift_client):
    pool = pingpong_thrift_client.pool
    for c in pool.connections:
        c.close()
    pool.connections = pool.QueueCls()

    def reset_pool():
        for c in pool.connections:
            c.close()
        pool.max_conn = 30
        pool.connections = pool.QueueCls()

    request.addfinalizer(reset_pool)


def test_client_pool(pingpong_thrift_client):
    old_client = None

    with pingpong_thrift_client.pool.connection_ctx() as c:
        c.ping()
        old_client = c

    with pingpong_thrift_client.pool.connection_ctx() as c:
        c.ping()
        assert c is old_client


def test_client_pool_dead_connection_occured(pingpong_thrift_client):
    os.kill(pingpong_thrift_client.process.pid, signal.SIGHUP)  # restart
    time.sleep(1)

    with pingpong_thrift_client.pool.connection_ctx() as c:
        c.ping()
        old_client = c

    with pingpong_thrift_client.pool.connection_ctx() as c:
        c.ping()
        assert c is old_client


def test_client_pool_disabled(pingpong_thrift_client, init_pingpong_pool):
    pool = pingpong_thrift_client.pool
    pool.max_conn = 0

    with pingpong_thrift_client.pool.connection_ctx() as c:
        c.ping()
        old_client = c

    with pingpong_thrift_client.pool.connection_ctx() as c:
        c.ping()
        assert c is not old_client


def test_client_pool_overflow(
        monkeypatch, pingpong_thrift_client, pingpong_service_key,
        init_pingpong_pool):

    pool = pingpong_thrift_client.pool
    pool.max_conn = 3

    monkeypatch.setattr(ClientPool, 'get_client_from_pool', lambda *args: None)

    available_clients = []
    for _ in range(5):
        with pingpong_thrift_client.pool.connection_ctx() as c:
            c.ping()
            available_clients.append(c)

    pool = pingpong_thrift_client.pool
    assert len(pool.connections) <= pool.max_conn

    for n, c in enumerate(available_clients[:pool.max_conn]):
        if n > pool.max_conn - 1:
            # Overflown connections should be closed after use.
            with pytest.raises(TTransportException):
                c.ping()
        else:
            c.ping()


def test_client_call_and_put_back(
        pingpong_service_key, pingpong_thrift_client, init_pingpong_pool):
    pool = pingpong_thrift_client.pool

    pool.ping()
    pool.ping()
    pool.ping()

    assert len(pool.connections) == 1
    conn = list(pool.connections)[0]

    for c in pool.connections:
        c.close()

    pool.ping()
    pool.ping()
    pool.ping()

    assert len(pool.connections) == 1
    assert conn is not list(pool.connections)[0]


def test_ttronsport_exception_not_put_back(
        pingpong_thrift_client, pingpong_service_key, init_pingpong_pool):

    pool = pingpong_thrift_client.pool

    with pingpong_thrift_client.pool.connection_ctx() as c:
        c.ping()

    assert len(pool.connections) == 1

    # If TTransportException occurs, conn shouldn't be put back into pool.
    with pytest.raises(TTransportException):
        with pingpong_thrift_client.pool.connection_ctx():
            raise TTransportException

    assert len(pool.connections) == 0

    with pingpong_thrift_client.pool.connection_ctx() as c:
        c.ping()
        old_client = c

    assert len(pool.connections) == 1

    # If predefined exception occurs, conn should be put back and available.
    with pytest.raises(
            pingpong_thrift_client.service.AboutToShutDownException):
        with pingpong_thrift_client.pool.connection_ctx() as c:
            raise pingpong_thrift_client.service.AboutToShutDownException

    with pingpong_thrift_client.pool.connection_ctx() as c:
        assert c is old_client
        c.ping()


def test_should_not_put_back_connection_if_ttransport_exception_raised(
        pingpong_thrift_client, pingpong_service_key, init_pingpong_pool):
    pool = pingpong_thrift_client.pool

    with pingpong_thrift_client.pool.connection_ctx() as c:
        c.ping()

    assert len(pool.connections) == 1
    c.should_fail_api = Mock(side_effect=c.TTransportException)
    with pytest.raises(c.TTransportException):
        pool.should_fail_api()

    assert len(pool.connections) == 0

    with pingpong_thrift_client.pool.connection_ctx() as c:
        c.ping()

    assert len(pool.connections) == 1
    c.should_fail_api = Mock(
        side_effect=pingpong_thrift_client.service.AboutToShutDownException)

    # If predefined exception occurs, conn should be put back and available.
    with pytest.raises(
            pingpong_thrift_client.service.AboutToShutDownException):
        pool.should_fail_api()

    assert len(pool.connections) == 1
    assert list(pool.connections)[0] == c
    pool.ping()


def test_setted_connection_pool_connection_keepalive(
        pingpong_thrift_client, pingpong_service_key, pingpong_thrift_service,
        fake_datetime):
    keep_alive = 1
    pool = ClientPool(
        pingpong_thrift_service,
        pingpong_thrift_client.host,
        pingpong_thrift_client.port,
        name=pingpong_service_key,
        raise_empty=False, max_conn=3,
        connection_class=pingpong_thrift_client.pool.connection_class,
        keepalive=keep_alive
    )
    assert pool.keepalive == keep_alive
    with pool.connection_ctx() as conn:
        now = time.time()
        assert conn.alive_until == now + keep_alive
        assert conn.test_connection()
        old_connection = conn

    fake_datetime.FAKE_TIME = now + 0.1
    with pool.connection_ctx() as conn:
        assert conn is old_connection

    fake_datetime.FAKE_TIME = now + keep_alive + 2
    assert not old_connection.test_connection()

    with pool.connection_ctx() as conn:
        assert old_connection is not conn


def test_not_setted_connection_pool_connection_keepalive(
        pingpong_thrift_client, pingpong_service_key, pingpong_thrift_service,
        fake_datetime):
    pool = ClientPool(
        pingpong_thrift_service,
        pingpong_thrift_client.host,
        pingpong_thrift_client.port,
        name=pingpong_service_key,
        raise_empty=False, max_conn=3,
        connection_class=pingpong_thrift_client.pool.connection_class,
    )
    assert pool.keepalive is None
    with pool.connection_ctx() as conn:
        now = time.time()
        assert conn.alive_until is None
        assert conn.test_connection()
        old_connection = conn

    fake_datetime.FAKE_TIME = now + 0.1
    with pool.connection_ctx() as conn:
        assert conn is old_connection

    fake_datetime.FAKE_TIME = now + timedelta(days=100).seconds
    assert old_connection.test_connection()

    with pool.connection_ctx() as conn:
        assert old_connection is conn


def test_connection_pool_generation(
        pingpong_thrift_client, pingpong_service_key, pingpong_thrift_service,
        fake_datetime):
    pool = ClientPool(
        pingpong_thrift_service,
        pingpong_thrift_client.host,
        pingpong_thrift_client.port,
        name=pingpong_service_key,
        raise_empty=False, max_conn=3,
        connection_class=pingpong_thrift_client.pool.connection_class,
    )
    c = pool.produce_client()
    assert c.pool_generation == pool.generation == 0

    pool.clear()

    c2 = pool.produce_client()
    assert c2.pool_generation == pool.generation == 1

    pool.put_back_connection(c)
    pool.put_back_connection(c2)

    for c in pool.connections:
        assert c.pool_generation == pool.generation


def test_random_multiconnection_pool(
        pingpong_thrift_client, pingpong_service_key, pingpong_thrift_service,
        fake_datetime):
    servers = [
        (pingpong_thrift_client.host, pingpong_thrift_client.port),
        (pingpong_thrift_client.host, pingpong_thrift_client.port2),
    ]

    random_pool = RandomMultiServerClient(
        pingpong_thrift_service,
        servers=servers,
        name=pingpong_service_key,
        raise_empty=False, max_conn=3,
        connection_class=pingpong_thrift_client.pool.connection_class,
    )

    with random_pool.connection_ctx() as conn:
        assert conn.test_connection()


def test_roundrobin_multiconnection_pool(
        pingpong_thrift_client, pingpong_service_key, pingpong_thrift_service,
        fake_datetime):
    servers = [
        (pingpong_thrift_client.host, pingpong_thrift_client.port),
        (pingpong_thrift_client.host, pingpong_thrift_client.port2),
    ]

    roundrobin_pool = RoundRobinMultiServerClient(
        pingpong_thrift_service,
        servers=servers,
        name=pingpong_service_key,
        raise_empty=False, max_conn=3,
        connection_class=pingpong_thrift_client.pool.connection_class,
    )

    conn1 = roundrobin_pool.produce_client()
    assert conn1.test_connection()

    conn2 = roundrobin_pool.produce_client()
    assert conn2.test_connection()
    assert (conn1.host, conn1.port) != (conn2.host, conn2.port)

    conn3 = roundrobin_pool.produce_client()
    assert (conn1.host, conn1.port) == (conn3.host, conn3.port)
    assert (conn2.host, conn2.port) != (conn3.host, conn3.port)

    conn4 = roundrobin_pool.produce_client()
    assert (conn1.host, conn1.port) != (conn4.host, conn4.port)
    assert (conn2.host, conn2.port) == (conn4.host, conn4.port)


def test_heartbeat_client_pool(
        pingpong_thrift_client, pingpong_service_key, pingpong_thrift_service):

    heartbeat_pool = HeartbeatClientPool(
        pingpong_thrift_service,
        host=pingpong_thrift_client.host,
        port=pingpong_thrift_client.port,
        timeout=1,
        connection_class=pingpong_thrift_client.pool.connection_class,
        max_conn=3,
        check_interval=2,
    )

    conn1 = heartbeat_pool.get_client()
    assert conn1.test_connection()

    # now we kill client and put back to pool
    conn1.close()
    heartbeat_pool.put_back_connection(conn1)
    assert heartbeat_pool.pool_size() == 1

    # this call should fail
    disconnected_client = heartbeat_pool.get_client()
    assert not disconnected_client.test_connection()
    assert heartbeat_pool.put_back_connection(disconnected_client)
    assert heartbeat_pool.pool_size() == 1

    time.sleep(1)
    assert heartbeat_pool.pool_size() == 1
    # after check_interval, connection need check, but connection is dead
    time.sleep(2)
    # disconnection should be detected and dead clients removed (1 client may not be counted if it is being checked)
    assert heartbeat_pool.pool_size() == 0

    for _ in range(3):
        conn = heartbeat_pool.produce_client()
        heartbeat_pool.put_back_connection(conn)

    time.sleep(4)

    # Make sure all clients have been checked
    use_counts = [client.use_count for client in heartbeat_pool.connections]
    assert all(use_counts)

    heartbeat_pool.clear()

    time.sleep(1)


def test_api_call_context(
        pingpong_thrift_client, pingpong_service_key, pingpong_thrift_service,
        fake_time):
    from thrift_connector.hooks import before_call, after_call

    mock_before_hook = Mock()
    mock_after_hook = Mock()
    before_call.register(mock_before_hook)
    after_call.register(mock_after_hook)

    pool = ClientPool(
        pingpong_thrift_service,
        pingpong_thrift_client.host,
        pingpong_thrift_client.port,
        name=pingpong_service_key,
        raise_empty=False, max_conn=3,
        connection_class=pingpong_thrift_client.pool.connection_class,
    )
    pool.ping()

    # get one client manually, there should be one client in pool,
    # since there's only one call
    client = pool.get_client()
    assert client.test_connection()
    pool.put_back_connection(client)

    mock_before_hook.assert_called_with(pool, client, 'ping', fake_time.time())
    mock_after_hook.assert_called_with(pool, client, 'ping', fake_time.time(),
                                       0, 'pong')

    # raise Exception when raises specified
    mock_before_hook_with_err = Mock(side_effect=TypeError('test'))
    before_call.register(mock_before_hook_with_err, raises=(TypeError,))
    with pool.connection_ctx() as client:
        with pytest.raises(TypeError) as exc_info:
            client.win()
        assert "test" in str(exc_info.value)
    before_call.callbacks.clear()
    after_call.callbacks.clear()

    mock_before_hook_with_err = Mock(side_effect=TypeError('test'))
    before_call.register(mock_before_hook_with_err, raises=[TypeError,
                                                            RuntimeError])
    with pool.connection_ctx() as client:
        with pytest.raises(TypeError) as exc_info:
            client.win()
        assert "test" in str(exc_info.value)
    before_call.callbacks.clear()
    after_call.callbacks.clear()

    mock_before_hook_with_err = Mock(side_effect=TypeError('test'))
    before_call.register(mock_before_hook_with_err, raises=(TypeError,
                                                            RuntimeError))
    with pool.connection_ctx() as client:
        with pytest.raises(TypeError) as exc_info:
            client.win()
        assert "test" in str(exc_info.value)
    before_call.callbacks.clear()
    after_call.callbacks.clear()

    before_call.register(mock_before_hook_with_err, raises=TypeError)
    with pool.connection_ctx() as client:
        with pytest.raises(TypeError) as exc_info:
            client.win()
        assert "test" in str(exc_info.value)
    before_call.callbacks.clear()
    after_call.callbacks.clear()


def test_conn_close_hook(pingpong_thrift_client, pingpong_service_key,
                         pingpong_thrift_service, fake_time):
    pool = ClientPool(
        pingpong_thrift_service,
        pingpong_thrift_client.host,
        pingpong_thrift_client.port,
        name=pingpong_service_key,
        raise_empty=False, max_conn=3,
        connection_class=pingpong_thrift_client.pool.connection_class,
    )
    close_mock = Mock()
    pool.register_after_close_func(close_mock)
    client = pool.get_client()
    client.close()
    close_mock.assert_called_with(pool, client)


def test_set_timeout(pingpong_thrift_client, pingpong_service_key,
                     pingpong_thrift_service, fake_time):
    pool = ClientPool(
        pingpong_thrift_service,
        pingpong_thrift_client.host,
        pingpong_thrift_client.port,
        name=pingpong_service_key,
        raise_empty=False, max_conn=3,
        connection_class=pingpong_thrift_client.pool.connection_class,
    )
    client = pool.get_client()

    client.set_client_timeout(0.5 * 1000)
    assert client.sleep(0.2) == 'good morning'

    with pytest.raises(socket.timeout) as e:
        client.sleep(1)
    assert 'timed out' in str(e.value)
    client.close()

    with pytest.raises(socket.timeout) as e:
        with pool.connection_ctx(timeout=1) as client:
            client.sleep(2)
    assert 'timed out' in str(e.value)


def test_fill_conneciont_pool(pingpong_thrift_client, pingpong_service_key,
                              pingpong_thrift_service, monkeypatch):
    pool = BaseClientPool(
        pingpong_thrift_service,
        connection_class=pingpong_thrift_client.pool.connection_class,
    )
    assert pool.max_conn == 30
    assert pool.pool_size() == 0
    pool.yield_server = Mock(
        return_value=(
            pingpong_thrift_client.host, pingpong_thrift_client.port))
    pool.fill_connection_pool()
    assert pool.pool_size() == pool.max_conn
