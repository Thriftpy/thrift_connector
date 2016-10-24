from __future__ import absolute_import

import contextlib
import logging
import random
import time

from datetime import timedelta
from tornado import gen
from tornado.concurrent import Future, is_future
from tornado.ioloop import PeriodicCallback

from thrift_connector.connection_pool import ThriftClient, BaseClientPool, ThriftPyBaseClient, \
    ClientPool, validate_host_port

logger = logging.getLogger(__name__)


class TornadoThriftClient(ThriftClient):
    @classmethod
    def get_socket_factory(self):
        return lambda host, port: [host, port]

    @classmethod
    def set_timeout(cls, socket, timeout):
        socket[2:] = [timeout]

    @classmethod
    def get_transport_factory(self):
        from thrift.TTornado import TTornadoStreamTransport
        return lambda socket: TTornadoStreamTransport(socket[0], socket[1])

    @classmethod
    def get_protoco_factory(self):
        from thrift.protocol import TBinaryProtocol
        return lambda _: TBinaryProtocol.TBinaryProtocolFactory()

    def get_tclient(self, service, protocol):
        if self.tracking is True:
            raise NotImplementedError(
                "%s doesn't support tracking" % self.__class__.__name__)
        return service.Client(self.transport, protocol)

    def get_timeout(self):
        return self.socket[2]

    def test_connection(self):
        future = Future()

        if self.is_expired() or self.is_tired():
            future.set_result(False)
            return future

        def cb(ping_future):
            try:
                ping_future.result()
                future.set_result(True)
            except:
                future.set_result(False)

        try:
            self.ping().add_done_callback(cb)
        except:
            future.set_result(False)

        return future


class TornadoThriftPyClient(ThriftPyBaseClient):
    @classmethod
    def get_socket_factory(cls):
        return lambda host, port: [host, port]

    @classmethod
    def set_timeout(cls, socket, timeout):
        socket[2:] = [timeout]

    @classmethod
    def get_transport_factory(cls):
        from thriftpy.tornado import TTornadoStreamTransport
        return lambda socket: TTornadoStreamTransport(
            socket[0], socket[1], read_timeout=timedelta(milliseconds=socket[2]))

    @classmethod
    def get_protoco_factory(self):
        # These imports bypass the cython binary
        from thriftpy.protocol.binary import TBinaryProtocolFactory
        from thriftpy.transport.memory import TMemoryBuffer

        factory = TBinaryProtocolFactory()
        return lambda transport: (
            factory.get_protocol(TMemoryBuffer()),
            factory.get_protocol(transport)
        )

    def get_tclient(self, service, protocol):
        if self.tracking is True:
            raise NotImplementedError(
                "%s doesn't support tracking" % self.__class__.__name__)

        from thriftpy.tornado import TTornadoClient
        return TTornadoClient(service, protocol[0], protocol[1])

    def set_client_timeout(self, timeout):
        super(TornadoThriftPyClient, self).set_client_timeout(timeout)

        self.transport.read_timeout = timedelta(milliseconds=timeout)

    def get_timeout(self):
        return self.socket[2]

    def test_connection(self):
        future = Future()

        if self.is_expired() or self.is_tired():
            future.set_result(False)
            return future

        def cb(ping_future):
            try:
                ping_future.result()
                future.set_result(True)
            except:
                future.set_result(False)

        try:
            self.ping().add_done_callback(cb)
        except:
            future.set_result(False)

        return future


class _ContextManagerFuture(Future):
    """A Future that can be used with the "with" statement.

    When a coroutine yields this Future, the return value is a context manager
    that can be used like:

        with (yield future) as target:
            do_something(target)

    The value returned by wrapped future will be bound to the target.
    At the end of the block, the Future's exit callback will be invoked
    with this value and the exception if caught.

    Inspired by `toro <https://github.com/ajdavis/toro>`_. The original code is licensed under
    Apache License, Version 2.0 (`LICENSE <https://raw.githubusercontent.com/ajdavis/toro/master/LICENSE>`_).
    """

    def __init__(self, wrapped, exit_callback):
        super(_ContextManagerFuture, self).__init__()
        wrapped.add_done_callback(self._done_callback)
        self.exit_callback = exit_callback

    def _done_callback(self, wrapped):
        if wrapped.exception():
            self.set_exc_info(wrapped.exc_info())
        else:
            self.set_result(wrapped.result())

    def result(self, timeout=None):
        if self.exception():
            super(_ContextManagerFuture, self).result()

        # Otherwise return a context manager that cleans up after the block.
        @contextlib.contextmanager
        def f():
            try:
                yield self._result
            except Exception as e:
                self.exit_callback(self._result, e)
            else:
                self.exit_callback(self._result, None)

        return f()


class TornadoBaseClientPool(BaseClientPool):
    def __init__(self, *args, **kwargs):
        super(TornadoBaseClientPool, self).__init__(*args, **kwargs)
        self.__api_method_cache = {}

    def get_client_from_pool(self):
        future = Future()

        connection = self._get_connection()
        if connection is None:
            future.set_result(None)
            return future

        def cb(test_future):
            if test_future.result():
                future.set_result(connection)
            else:
                connection.close()
                future.set_result(None)

        connection.test_connection().add_done_callback(cb)  # make sure old connection is usable

        return future

    @gen.coroutine
    def get_client(self):
        raise gen.Return((yield self.get_client_from_pool()) or self.produce_client())

    def __getattr__(self, name):
        method = self.__api_method_cache.get(name)
        if not method:
            @gen.coroutine
            def method(*args, **kwds):
                client = yield self.get_client()
                api = getattr(client, name, None)
                will_put_back = True
                try:
                    if api and (callable(api) or is_future(api)):
                        raise gen.Return((yield api(*args, **kwds)))
                    raise AttributeError("%s not found in %s" % (name, client))
                except client.TTransportException:
                    will_put_back = False
                    client.close()
                    raise
                finally:
                    if will_put_back:
                        self.put_back_connection(client)

            self.__api_method_cache[name] = method
        return method

    def _connection_ctx_exit_callback(self, client, exception):
        if not exception:
            self.put_back_connection(client)
            return

        if isinstance(exception, client.TTransportException):
            client.close()
            raise
        else:
            self.put_back_connection(client)
            raise

    def connection_ctx(self, timeout=None):
        future = self.get_client()

        if timeout is not None:
            future.add_done_callback(lambda f: f.result().set_client_timeout(timeout * 1000))

        return _ContextManagerFuture(future, self._connection_ctx_exit_callback)


class TornadoClientPool(TornadoBaseClientPool, ClientPool):
    pass


class TornadoHeartbeatClientPool(TornadoClientPool):
    def __init__(self, service, host, port, timeout=30, name=None,
                 raise_empty=False, max_conn=30, connection_class=ThriftClient,
                 keepalive=None, tracking=False, tracker_factory=None,
                 use_limit=None, check_interval=10):
        super(TornadoHeartbeatClientPool, self).__init__(
            service=service,
            host=host,
            port=port,
            timeout=timeout,
            name=name,
            raise_empty=raise_empty,
            max_conn=max_conn,
            connection_class=connection_class,
            keepalive=keepalive,
            tracking=tracking,
            tracker_factory=tracker_factory,
            use_limit=use_limit,
        )
        self.check_interval = check_interval

        self._heartbeat_timer = PeriodicCallback(self.maintain_connections, max(1, self.timeout - 5) * 1000)
        self._heartbeat_timer.start()

    def get_client_from_pool(self):
        future = Future()
        future.set_result(self._get_connection())
        return future

    @gen.coroutine
    def maintain_connections(self):
        pool_size = self.pool_size()
        for _ in range(pool_size):
            conn = self._get_connection()
            if conn is None:
                break

            if time.time() - conn.latest_use_time < self.check_interval \
                    or (yield conn.test_connection()):
                self.put_back_connection(conn)
            else:
                conn.close()


class TornadoMultiServerClientBase(TornadoBaseClientPool):
    def __init__(self, service, servers, timeout=30, name=None,
                 raise_empty=False, max_conn=30, connection_class=ThriftClient,
                 keepalive=None, tracking=False, tracker_factory=None,
                 use_limit=None):
        super(TornadoMultiServerClientBase, self).__init__(
            service=service,
            timeout=timeout,
            name=name,
            raise_empty=raise_empty,
            max_conn=max_conn,
            connection_class=connection_class,
            keepalive=keepalive,
            tracking=tracking,
            tracker_factory=None,
            use_limit=use_limit,
        )

        self.servers = list(servers)

    def set_servers(self, server_info):
        for i in server_info:
            assert len(i) == 2
            validate_host_port(*i)
        self.servers = server_info
        self.clear()


class TornadoRandomMultiServerClient(TornadoMultiServerClientBase):
    def yield_server(self):
        assert len(self.servers) > 0
        return random.choice(self.servers)


class TornadoRoundRobinMultiServerClient(TornadoMultiServerClientBase):
    def __init__(self, *args, **kwds):
        super(TornadoRoundRobinMultiServerClient, self).__init__(*args, **kwds)
        self.index = random.randint(0, len(self.servers) - 1)
        random.shuffle(self.servers)

    def yield_server(self):
        assert len(self.servers) > 0
        if self.index >= len(self.servers):
            self.index = 0
        ret = self.servers[self.index]
        self.index += 1
        return ret
