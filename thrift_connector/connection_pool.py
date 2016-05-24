# -*- coding: utf-8 -*-

import logging
import contextlib
import random
import threading
import time

from .hooks import api_call_context

logger = logging.getLogger(__name__)

SIGNAL_CLOSE_NAME = "close"


class ConnectionError(Exception):
    pass


def validate_host_port(host, port):
    if not all((host, port)):
        raise RuntimeError("host and port not valid: %r:%r" % (host, port))


class ThriftBaseClient(object):
    def __init__(self, host, port, transport, protocol, service,
                 keepalive=None, pool_generation=0, tracking=False,
                 tracker_factory=None, pool=None, socket=None, use_limit=None):
        self.host = host
        self.port = port
        self.transport = transport
        self.protocol = protocol
        self.service = service
        self.keepalive = keepalive
        self.alive_until = time.time() + keepalive if keepalive else None
        self.use_count = 0
        self.use_limit = use_limit
        self.pool_generation = pool_generation
        self.tracking = tracking
        self.tracker_factory = tracker_factory
        self.socket = socket
        self.pool = pool

        self.client = self.get_tclient(service, protocol)
        self.init_client(self.client)

    def __repr__(self):
        return "<%s service=%s>" % (
            self.__class__.__name__,
            self.client.__class__.__module__
        )

    def __getattr__(self, name):
        return getattr(self.client, name)

    def init_client(self, client):
        pass

    def close(self, callback=None):
        if callback is not None:
            callback(self)
        try:
            self.transport.close()
        except Exception as e:
            logger.warn("Connction close failed: %r" % e)
        finally:
            self.pool.signal_handler(SIGNAL_CLOSE_NAME, self)

    def is_expired(self):
        now = time.time()
        return (self.alive_until and now > self.alive_until and
                random.random() < (now - self.alive_until)/self.keepalive)

    def incr_use_count(self):
        self.use_count += 1

    def is_tired(self):
        return self.use_limit and self.use_count > self.use_limit

    def test_connection(self):
        if self.is_expired() or self.is_tired():
            return False
        try:
            self.ping()
            return True
        except:
            return False

    @classmethod
    def connect(cls, service, host, port, timeout=30, keepalive=None,
                pool_generation=0, tracking=False, tracker_factory=None,
                pool=None, use_limit=None):
        SOCKET = cls.get_socket_factory()(host, port)
        cls.set_timeout(SOCKET, timeout * 1000)
        PROTO_FACTORY = cls.get_protoco_factory()
        TRANS_FACTORY = cls.get_transport_factory()

        transport = TRANS_FACTORY(SOCKET)
        protocol = PROTO_FACTORY(transport)

        transport.open()

        return cls(
            host=host,
            port=port,
            transport=transport,
            protocol=protocol,
            service=service,
            keepalive=keepalive,
            pool_generation=pool_generation,
            tracking=tracking,
            tracker_factory=tracker_factory,
            pool=pool,
            socket=SOCKET,
            use_limit=use_limit,
        )

    @property
    def TTransportException(self):
        raise NotImplementedError

    @classmethod
    def get_protoco_factory(self):
        raise NotImplementedError

    @classmethod
    def get_transport_factory(self):
        raise NotImplementedError

    def get_tclient(self, service, protocol):
        raise NotImplementedError

    @classmethod
    def get_socket_factory(self):
        raise NotImplementedError

    @classmethod
    def set_timeout(cls, socket, timeout):
        raise NotImplementedError

    def set_client_timeout(self, timeout):
        self.set_timeout(self.socket, timeout)

    def get_timeout(self):
        raise NotImplementedError


class ThriftClient(ThriftBaseClient):
    def init_client(self, client):
        for api in dir(client):
            if not api.startswith(('_', '__', 'send_', 'recv_')):
                target = getattr(client, api)
                setattr(client, api,
                        api_call_context(self.pool, self, api)(target))

    @property
    def TTransportException(self):
        from thrift.transport.TTransport import TTransportException
        return TTransportException

    @classmethod
    def get_protoco_factory(self):
        from thrift.protocol import TBinaryProtocol
        return TBinaryProtocol.TBinaryProtocolAccelerated

    @classmethod
    def get_transport_factory(self):
        from thrift.transport import TTransport
        return TTransport.TBufferedTransport

    def get_tclient(self, service, protocol):
        if self.tracking is True:
            raise NotImplementedError(
                "%s doesn't support tracking" % self.__class__.__name__)
        return service.Client(protocol)

    @classmethod
    def get_socket_factory(self):
        from thrift.transport import TSocket
        return TSocket.TSocket

    @classmethod
    def set_timeout(cls, socket, timeout):
        socket.setTimeout(timeout)

    def get_timeout(self):
        return self.socket._timeout


class ThriftPyBaseClient(ThriftBaseClient):
    def init_client(self, client):
        for api in self.service.thrift_services:
            target = getattr(client, api)
            setattr(client, api,
                    api_call_context(self.pool, self, api)(target))

    @property
    def TTransportException(self):
        from thriftpy.transport import TTransportException
        return TTransportException

    def get_tclient(self, service, protocol):
        if self.tracking is True:
            from thriftpy.contrib.tracking import TTrackedClient
            client = TTrackedClient(self.tracker_factory, service, protocol)
        else:
            from thriftpy.thrift import TClient
            client = TClient(service, protocol)
        return client

    @classmethod
    def get_socket_factory(self):
        from thriftpy.transport import TSocket
        return TSocket

    @classmethod
    def set_timeout(cls, socket, timeout):
        socket.set_timeout(timeout)

    def get_timeout(self):
        return self.socket.socket_timeout


class ThriftPyClient(ThriftPyBaseClient):
    @classmethod
    def get_protoco_factory(self):
        from thriftpy.protocol import TBinaryProtocolFactory
        return TBinaryProtocolFactory().get_protocol

    @classmethod
    def get_transport_factory(self):
        from thriftpy.transport import TBufferedTransportFactory
        return TBufferedTransportFactory().get_transport


class ThriftPyCyClient(ThriftPyBaseClient):
    @classmethod
    def get_protoco_factory(self):
        from thriftpy.protocol import TCyBinaryProtocolFactory
        return TCyBinaryProtocolFactory().get_protocol

    @classmethod
    def get_transport_factory(self):
        from thriftpy.transport import TCyBufferedTransportFactory
        return TCyBufferedTransportFactory().get_transport


class BaseClientPool(object):
    def __init__(self, service, timeout=30, name=None, raise_empty=False,
                 max_conn=30, connction_class=ThriftClient, keepalive=None,
                 tracking=False, tracker_factory=None, use_limit=None):
        if service is None:
            raise RuntimeError("Service cannot be None")

        self.service = service
        self.timeout = timeout
        self.name = name or service.__name__
        self.connections = set()
        self.raise_empty = raise_empty
        self.max_conn = max_conn
        self.connction_class = connction_class
        self.keepalive = keepalive
        self.use_limit = use_limit
        self.generation = 0
        self.tracking = tracking
        self.tracker_factory = tracker_factory
        self.conn_close_callbacks = []
        self.__api_method_cache = {}

    @contextlib.contextmanager
    def annotate(self, **kwds):
        if not self.tracking:
            raise NotImplementedError("Tracking is not enabled")

        with self.tracker_factory.annotate(**kwds) as annotation:
            yield annotation

    def keys(self):
        return set([self.name, self.service.__name__])

    def __repr__(self):
        return "<%s service=%r>" % (
            self.__class__.__name__,
            self.keys()
        )

    def pool_size(self):
        return len(self.connections)

    def remove_from_pools(self, conn):
        self.connections.remove(conn)

    def clear(self):
        old_connections = self.connections
        self.connections.clear()
        self.generation += 1

        for c in old_connections:
            c.close()

    def get_client_from_pool(self):
        connection = self._get_connection()
        if connection is None:
            return
        if connection.test_connection():  # make sure old connection is usable
            return connection
        else:
            connection.close(callback=self.remove_from_pools)

    def _get_connection(self):
        try:
            return self.connections.pop()
        # When only one connection left, just return None if it
        # has already been popped in another thread.
        except KeyError:
            conn = self.produce_client()
            return conn

    def put_back_connection(self, conn):
        assert isinstance(conn, ThriftBaseClient)
        if conn.pool_generation == self.generation:
            if self.timeout != conn.get_timeout():
                conn.set_client_timeout(self.timeout * 1000)
            self.connections.add(conn)
            return True
        else:
            conn.close()
            return False

    def produce_client(self, host=None, port=None):
        if host is None and port is None:
            host, port = self.yield_server()
        elif not all((host, port)):
            raise ValueError("host and port should be 'both none' \
                             or 'both provided' ")
        if self.pool_size >= self.max_conn:
            raise ConnectionError('Too many connections')
        conn = self.connction_class.connect(
            self.service,
            host,
            port,
            self.timeout,
            keepalive=self.keepalive,
            pool_generation=self.generation,
            tracking=self.tracking,
            tracker_factory=self.tracker_factory,
            pool=self,
            use_limit=self.use_limit,
        )
        res = self.put_back_connection(conn)
        if not res:
            logger.warning('Pool has been cleared.')
        return conn

    def get_client(self):
        return self.get_client_from_pool()

    def __getattr__(self, name):
        method = self.__api_method_cache.get(name)
        if not method:
            def method(*args, **kwds):
                client = self.get_client()
                api = getattr(client, name, None)
                try:
                    if api and callable(api):
                        return api(*args, **kwds)
                    raise AttributeError("%s not found in %s" % (name, client))
                except client.TTransportException:
                    client.close(callback=self.remove_from_pools)
                    raise
            self.__api_method_cache[name] = method
        return method

    @contextlib.contextmanager
    def connection_ctx(self, timeout=None):
        client = self.get_client()
        if timeout is not None:
            client.set_client_timeout(timeout * 1000)
        try:
            yield client
        except client.TTransportException:
            client.close(callback=self.remove_from_pools)
            raise
        except Exception:
            raise

    @contextlib.contextmanager
    def make_temporary_client(self, host, port):
        client = self.produce_client(host, port)
        try:
            yield client
        except Exception:
            raise
        finally:
            client.close(callback=self.remove_from_pools)

    def register_after_close_func(self, func):
        self.conn_close_callbacks.append(func)

    def signal_handler(self, signal_name, conn):
        if signal_name == SIGNAL_CLOSE_NAME:
            for cb in self.conn_close_callbacks:
                try:
                    cb(self, conn)
                except:
                    logger.warn("%s Callback failed" % SIGNAL_CLOSE_NAME,
                                exc_info=True)


class ClientPool(BaseClientPool):
    def __init__(self, service, host, port, timeout=30, name=None,
                 raise_empty=False, max_conn=30, connction_class=ThriftClient,
                 keepalive=None, tracking=False, tracker_factory=None,
                 use_limit=None):
        validate_host_port(host, port)
        super(ClientPool, self).__init__(
            service=service,
            timeout=timeout,
            name=name,
            raise_empty=raise_empty,
            max_conn=max_conn,
            connction_class=connction_class,
            keepalive=keepalive,
            tracking=tracking,
            tracker_factory=tracker_factory,
            use_limit=use_limit,
        )
        self.host = host
        self.port = port

    def set_servers(self, server_info):
        host, port = server_info
        validate_host_port(host, port)
        self.host = host
        self.port = port
        self.clear()

    def yield_server(self):
        return self.host, self.port


class HeartbeatClientPool(ClientPool):

    def __init__(self, service, host, port, timeout=30, name=None,
                 raise_empty=False, max_conn=30, connction_class=ThriftClient,
                 keepalive=None, tracking=False, tracker_factory=None,
                 use_limit=None):
        super(HeartbeatClientPool, self).__init__(
            service=service,
            host=host,
            port=port,
            timeout=timeout,
            name=name,
            raise_empty=raise_empty,
            max_conn=max_conn,
            connction_class=connction_class,
            keepalive=keepalive,
            tracking=tracking,
            tracker_factory=tracker_factory,
            use_limit=use_limit,
        )
        threading.Thread(target=self.maintain_connections).start()

    def _close_and_remove_client(self, client):
        if client not in self.connections:
            return

        try:
            client.close(callback=self.remove_from_pools)
        except KeyError as e:
            logger.warn('Error removing client from pool %s, %s',
                        self.service.__name__, e)
        except Exception as e:
            logger.warn('Error closing client %s, %s',
                        self.service.__name__, e)

    def get_client_from_pool(self):
        return self._get_connection()

    def maintain_connections(self):
        sleep_time = max([1, self.timeout - 5])
        while True:
            time.sleep(sleep_time)
            count = 0
            pool_size = self.pool_size()

            while count < pool_size:
                conn = self.get_client_from_pool()

                if conn is None:
                    break

                count += 1

                if not conn.test_connection():
                    conn.close(callback=self.remove_from_pools)


class MultiServerClientBase(BaseClientPool):
    def __init__(self, service, servers, timeout=30, name=None,
                 raise_empty=False, max_conn=30, connction_class=ThriftClient,
                 keepalive=None, tracking=False, tracker_factory=None,
                 use_limit=None):
        super(MultiServerClientBase, self).__init__(
            service=service,
            timeout=timeout,
            name=name,
            raise_empty=raise_empty,
            max_conn=max_conn,
            connction_class=connction_class,
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


class RandomMultiServerClient(MultiServerClientBase):
    def yield_server(self):
        assert len(self.servers) > 0
        return random.choice(self.servers)


class RoundRobinMultiServerClient(MultiServerClientBase):
    def __init__(self, *args, **kwds):
        super(RoundRobinMultiServerClient, self).__init__(*args, **kwds)
        self.index = random.randint(0, len(self.servers) - 1)
        random.shuffle(self.servers)

    def yield_server(self):
        assert len(self.servers) > 0
        if self.index >= len(self.servers):
            self.index = 0
        ret = self.servers[self.index]
        self.index += 1
        return ret
