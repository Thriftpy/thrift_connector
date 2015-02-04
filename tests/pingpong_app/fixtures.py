# -*- coding: utf-8 -*-

import os
import signal
import time
import pytest
import subprocess
import random

current_path = os.path.dirname(os.path.abspath(__file__))


def worker_term(worker):
    os.environ['about_to_shutdown'] = "1"


@pytest.fixture(scope="session")
def pingpong_thrift_service(request, pingpong_service_key):
    import thriftpy
    thrift_service = thriftpy.load(
        os.path.join(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(__file__)
                    )
                ),
            "examples/pingpong_app/pingpong.thrift"),
        "pingpong_thrift")
    service = thrift_service.PingService
    service.AboutToShutDownException = \
        thrift_service.AboutToShutDownException
    return service


class TestServerInfo(object):
    def __init__(self, host, port, process, pool, service, port2):
        self.host = host
        self.port = port
        self.process = process
        self.pool = pool
        self.service = service
        self.port2 = port2


@pytest.fixture(scope="session")
def pingpong_thrift_client(request, pingpong_service_key,
                           pingpong_thrift_service):
    port = random.randint(55536, 65536)
    port2 = random.randint(35536, 45536)
    config_path = "examples/gunicorn_config.py"
    gunicorn_server = subprocess.Popen(
        ["gunicorn_thrift", "examples.pingpong_app.app:app",
            "-c", config_path, "--bind", "0.0.0.0:%s" % port,
            "--bind", "0.0.0.0:%s" % port2, ]
        )

    def shutdown():
        os.kill(gunicorn_server.pid, signal.SIGTERM)

    request.addfinalizer(shutdown)
    time.sleep(4)

    from thrift_connector import ClientPool, ThriftPyCyClient

    pool = ClientPool(
        pingpong_thrift_service,
        'localhost',
        port,
        name=pingpong_service_key,
        connection_class=ThriftPyCyClient
        )

    return TestServerInfo(
        'localhost',
        port,
        gunicorn_server,
        pool,
        pingpong_thrift_service,
        port2=port2
        )


@pytest.fixture(scope="session")
def pingpong_service_key():
    return 'test_pingpong'
