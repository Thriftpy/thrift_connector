Thrift Connector
================

Simple connection pool for thrift.

Usage
-----

thrift_connector can be used both for navive thrift client and thriftpy client with minor difference in usage.

Examples for thriftpy is:

.. code:: python

    import thriftpy
    from thrift_connector import ClientPool, ThriftPyCyClient

    service = thriftpy.load("pingpong_app/pingpong.thrift")
    pool = ClientPool(
        service.PingService,
        'localhost',
        8880,
        connction_class=ThriftPyCyClient
        )

    print "Sending Ping..."
    print "Receive:", pool.ping()
    print "Winning the match..."
    print "Receive:", pool.win()

Examples for thrift is:

.. code:: python

    # -*- coding: utf-8 -*-

    from pingpong_app.pingpong_sdk.pingpong import PingService
    from thrift_connector import ClientPool, ThriftClient

    pool = ClientPool(
        PingService,
        'localhost',
        8880,
        connction_class=ThriftClient
        )

    print "Sending Ping..."
    print "Receive:", pool.ping()
    print "Winning the match..."
    print "Receive:", pool.win()
