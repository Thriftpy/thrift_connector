Thrift Connector
================

Simple connection pool for thrift. thrift_connector can be used both for
native thrift client and thriftpy client with minor difference in usage.

Examples can be found in `examples` as well as the following sections.


Options
-------

service
    Defined thrift service. It should be the container class of apis for thriftpy (`thriftpy.load('...').XXXService`), and api module for thrift (`XXX_sdk.xxx.xxxService`)
host
    Server host
port
    Server port
[timeout]
    Socket timeout, in seconds.
[name]
    Connection pool name, for identity.
[raise_empty]
    Whether to raise exception if pool is empty while trying to obtain a connection.
[max_conn]
    Number of connections to manage in pool.
[connction_class]
    Connection class implementation. Builtin classes are: `ThriftClient` for native thrift, `ThriftPyClient` and `ThriftPyCyClient` for thriftpy, the latter one utilizes Cython for better performance.
[keepalive]
    Seconds each connection is able to stay alive. If oen connection has lived longer than this period, it will be closed.

Usage
-----


Examples for thriftpy is:

.. code:: python

    import thriftpy
    import thrift_connector.connection_pool as connection_pool

    service = thriftpy.load("pingpong_app/pingpong.thrift")
    pool = connection_pool.ClientPool(
        service.PingService,
        'localhost',
        8880,
        connction_class=connection_pool.ThriftPyCyClient
        )

    print "Sending Ping..."
    print "Receive:", pool.ping()
    print "Winning the match..."
    print "Receive:", pool.win()

Examples for thrift is:

.. code:: python

    # -*- coding: utf-8 -*-

    from pingpong_app.pingpong_sdk.pingpong import PingService
    import connection_pool

    pool = connection_pool.ClientPool(
        PingService,
        'localhost',
        8880,
        connction_class=connection_pool.ThriftClient
        )

    print "Sending Ping..."
    print "Receive:", pool.ping()
    print "Winning the match..."
    print "Receive:", pool.win()
