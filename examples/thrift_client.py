# -*- coding: utf-8 -*-

from pingpong_app.pingpong_sdk.pingpong import PingService
from thrift_connector import ClientPool, ThriftClient

pool = ClientPool(
    PingService,
    'localhost',
    8880,
    connection_class=ThriftClient
    )

print "Sending Ping..."
print "Receive:", pool.ping()
print "Winning the match..."
print "Receive:", pool.win()
