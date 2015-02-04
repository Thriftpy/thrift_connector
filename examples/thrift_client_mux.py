# -*- coding: utf-8 -*-

from pingpong_app.pingpong_sdk.pingpong import PingService
from thrift_connector import ClientPoolMux, ThriftClientMux

pool = ClientPoolMux(
    host='localhost',
    port=9090,
    services={'Ping': PingService},
    connection_class=ThriftClientMux
)

pool.ping('Ping')
