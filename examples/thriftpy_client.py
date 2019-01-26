# -*- coding: utf-8 -*-
from __future__ import print_function

import thriftpy2
from thrift_connector import ClientPool, ThriftPyCyClient

service = thriftpy2.load("pingpong_app/pingpong.thrift")
pool = ClientPool(
    service.PingService,
    'localhost',
    8880,
    connection_class=ThriftPyCyClient
    )

print("Sending Ping...")
print("Receive:", pool.ping())
print("Winning the match...")
print("Receive:", pool.win())
