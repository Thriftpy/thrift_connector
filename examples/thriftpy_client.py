# -*- coding: utf-8 -*-

import thriftpy
from thrift_connector import ClientPool, ThriftPyCyClient

service = thriftpy.load("pingpong_app/pingpong.thrift")
pool = ClientPool(
    service.PingService,
    'localhost',
    8880,
    connection_class=ThriftPyCyClient
    )

print "Sending Ping..."
print "Receive:", pool.ping()
print "Winning the match..."
print "Receive:", pool.win()
