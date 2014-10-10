# -*- coding: utf-8 -*-

import os
import time
import datetime
import signal

import pytest

from thrift_connector import ClientPool, connction_class
from thrift.transport.TTransport import TTransportException


@pytest.fixture
def fake_time(monkeypatch):
    class mock_datetime(object):
        FAKE_TIME = datetime.datetime(2014, 10, 9)

        @classmethod
        def now(cls):
            return cls.FAKE_TIME
    monkeypatch.setattr(datetime, 'datetime', mock_datetime)
    return mock_datetime


@pytest.fixture
def init_pingpong_pool(request, pingpong_service_key, pingpong_thrift_client):
    pool = pingpong_thrift_client.pool
    for c in pool.connections:
        c.close()
    pool.connections = set()

    def reset_pool():
        for c in pool.connections:
            c.close()
        pool.max_conn = 30
        pool.connections = set()

    request.addfinalizer(reset_pool)


@pytest.fixture
def register_with_pool_manager(
        request, pingpong_thrift_client, pingpong_service_key,
        pingpong_thrift_service):
    from zeus.core.client import make_client
    pool_manager.add_connection_pool(
        pingpong_thrift_client.service,
        pingpong_thrift_client.host,
        pingpong_thrift_client.port,
        name=pingpong_service_key,
        )

    def client(
            service=pingpong_thrift_service, host=pingpong_thrift_client.host,
            port=pingpong_thrift_client.port, timeout=30):
        return make_client(service, host, port)

    def remove():
        pool_manager.connection_pools = {}

    request.addfinalizer(remove)
    return client


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


def test_connection_pool_manager(
        pingpong_thrift_client, register_with_pool_manager,
        pingpong_service_key):
    pool = pool_manager[pingpong_service_key]
    pool.ping()

    assert pool.service == pingpong_thrift_client.service
    assert pool.host == pingpong_thrift_client.host
    assert pool.port == pingpong_thrift_client.port
    assert pool.name == pingpong_thrift_client.pool.name
    assert pool.connction_class == pingpong_thrift_client.pool.connction_class


def test_non_configured_client_should_be_temporarily_produced(
        pingpong_thrift_client, pingpong_service_key, init_pingpong_pool,
        register_with_pool_manager
        ):
    pool = pool_manager[pingpong_service_key]

    client = register_with_pool_manager
    # configured host is localhost
    with client(host='127.0.0.1') as c:
        c.ping()

    assert len(pool.connections) == 0

    # configured host is localhost
    with client() as c:
        c.ping()

    assert len(pool.connections) == 1


def test_setted_connection_pool_connection_keepalive(
        pingpong_thrift_client, pingpong_service_key, pingpong_thrift_service,
        fake_time):
    keep_alive = 1
    pool = ClientPool(
        pingpong_thrift_service,
        pingpong_thrift_client.host,
        pingpong_thrift_client.port,
        name=pingpong_service_key,
        raise_empty=False, max_conn=3, connction_class=connction_class,
        keepalive=keep_alive
        )
    assert pool.keepalive == keep_alive
    with pool.connection_ctx() as conn:
        now = datetime.datetime.now()
        assert conn.alive_until == now + datetime.timedelta(seconds=keep_alive)
        assert conn.test_connection()
        old_connection = conn

    fake_time.FAKE_TIME = now + datetime.timedelta(seconds=0.1)
    with pool.connection_ctx() as conn:
        assert conn is old_connection

    fake_time.FAKE_TIME = now + datetime.timedelta(seconds=keep_alive + 1)
    assert not old_connection.test_connection()

    with pool.connection_ctx() as conn:
        assert old_connection is not conn


def test_not_setted_connection_pool_connection_keepalive(
        pingpong_thrift_client, pingpong_service_key, pingpong_thrift_service,
        fake_time):
    pool = ClientPool(
        pingpong_thrift_service,
        pingpong_thrift_client.host,
        pingpong_thrift_client.port,
        name=pingpong_service_key,
        raise_empty=False, max_conn=3, connction_class=connction_class,
        )
    assert pool.keepalive is None
    with pool.connection_ctx() as conn:
        now = datetime.datetime.now()
        assert conn.alive_until is None
        assert conn.test_connection()
        old_connection = conn

    fake_time.FAKE_TIME = now + datetime.timedelta(seconds=0.1)
    with pool.connection_ctx() as conn:
        assert conn is old_connection

    fake_time.FAKE_TIME = now + datetime.timedelta(days=100)
    assert old_connection.test_connection()

    with pool.connection_ctx() as conn:
        assert old_connection is conn
