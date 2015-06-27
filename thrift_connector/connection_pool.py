# -*- coding: utf-8 -*-

import logging
import datetime
import contextlib
import random
import thread
import time

from .hooks import api_call_context

logger = logging.getLogger(__name__)

SIGNAL_CLOSE_NAME = "close"


def validate_host_port(host, port):
    if not all((host, port)):
        raise RuntimeError("host and port not valid: %r:%r" % (host, port))


class ThriftBaseClient(object):
    def __init__(self, host, port, transport, protocol, service,
                 keepalive=None, pool_generation=0, tracking=False,
                 tracker_factory=None, pool=None):
        self.host = host
        self.port = port
        self.transport = transport
        self.protocol = protocol
        self.service = service
        self.alive_until = datetime.datetime.now() + \
            datetime.timedelta(seconds=keepalive) if keepalive else None
        self.pool_generation = pool_generation
        self.tracking = tracking
        self.tracker_factory = tracker_factory

        self.client = self.get_tclient(service, protocol)
        self.pool = pool

    def __repr__(self):
        return "<%s service=%s>" % (
            self.__class__.__name__,
            self.client.__class__.__module__
            )

    def __getattr__(self, name):
        return getattr(self.client, name)

    def close(self):
        try:
            self.transport.close()
        except Exception as e:
            logger.warn("Connction close failed: %r" % e)
        finally:
            self.pool.signal_handler(SIGNAL_CLOSE_NAME, self)

    def is_expired(self):
        return self.alive_until and datetime.datetime.now() > self.alive_until

    def test_connection(self):
        if self.is_expired():
            return False
        try:
            self.ping()
            return True
        except:
            return False

    @classmethod
    def connect(cls, service, host, port, timeout=30, keepalive=None,
                pool_generation=0, tracking=False, tracker_factory=None,
                pool=None):
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


class ThriftClient(ThriftBaseClient):
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


class ThriftPyClient(ThriftBaseClient):
    @property
    def TTransportException(self):
        from thriftpy.transport import TTransportException
        return TTransportException

    @classmethod
    def get_protoco_factory(self):
        from thriftpy.protocol import TBinaryProtocolFactory
        return TBinaryProtocolFactory().get_protocol

    @classmethod
    def get_transport_factory(self):
        from thriftpy.transport import TBufferedTransportFactory
        return TBufferedTransportFactory().get_transport

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


class ThriftPyCyClient(ThriftBaseClient):
    @property
    def TTransportException(self):
        from thriftpy.transport import TTransportException
        return TTransportException

    @classmethod
    def get_protoco_factory(self):
        from thriftpy.protocol import TCyBinaryProtocolFactory
        return TCyBinaryProtocolFactory().get_protocol

    @classmethod
    def get_transport_factory(self):
        from thriftpy.transport import TCyBufferedTransportFactory
        return TCyBufferedTransportFactory().get_transport

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


class BaseClientPool(object):
    def __init__(self, service, timeout=30, name=None,
                 raise_empty=False, max_conn=30, connction_class=ThriftClient,
                 keepalive=None, tracking=False, tracker_factory=None):
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
        self.generation = 0
        self.tracking = tracking
        self.tracker_factory = tracker_factory
        self.conn_close_callbacks = []

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

    def clear(self):
        old_connections = self.connections
        self.connections = set()
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
            connection.close()

    def _get_connection(self):
        if not self.connections:
            if self.raise_empty:
                raise self.Empty
            return None
        try:
            return self.connections.pop()
        # When only one connection left, just return None if it
        # has already been popped in another thread.
        except KeyError:
            return None

    def put_back_connection(self, conn):
        assert isinstance(conn, ThriftBaseClient)
        if self.max_conn > 0 and len(self.connections) < self.max_conn and\
                conn.pool_generation == self.generation:
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
        return self.connction_class.connect(
            self.service,
            host,
            port,
            self.timeout,
            keepalive=self.keepalive,
            pool_generation=self.generation,
            tracking=self.tracking,
            tracker_factory=self.tracker_factory,
            pool=self
            )

    def get_client(self):
        return self.get_client_from_pool() or self.produce_client()

    def __getattr__(self, name):
        def call(*args, **kwds):
            client = self.get_client()
            api = getattr(client, name, None)
            try:
                if api and callable(api):
                    with api_call_context(self, client, name):
                        return api(*args, **kwds)
                raise AttributeError("%s not found in %s" % (name, client))
            finally:
                self.put_back_connection(client)
        return call

    @contextlib.contextmanager
    def connection_ctx(self):
        client = self.get_client()
        try:
            yield client
            self.put_back_connection(client)
        except client.TTransportException:
            client.close()
            raise
        except Exception:
            self.put_back_connection(client)
            raise

    @contextlib.contextmanager
    def make_temporary_client(self, host, port):
        client = self.produce_client(host, port)
        try:
            yield client
        except Exception:
            raise
        finally:
            client.close()

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
                 keepalive=None, tracking=False, tracker_factory=None):
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
                 keepalive=None, tracking=False, tracker_factory=None):
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
            tracker_factory=tracker_factory
            )
        thread.start_new_thread(self.maintain_connections, tuple())

    def _close_and_remove_client(self, client):
        if client not in self.connections:
            return

        try:
            self.connections.remove(client)
            client.close()
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

                if conn.test_connection():
                    self.put_back_connection(conn)
                else:
                    conn.close()


class MultiServerClientBase(BaseClientPool):
    def __init__(self, service, servers, timeout=30, name=None,
                 raise_empty=False, max_conn=30, connction_class=ThriftClient,
                 keepalive=None, tracking=False, tracker_factory=None):
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


class HuskarClientPool(BaseClientPool):
    class ServerInstance(object):
        def __init__(self, ip, port):
            self.ip = ip
            self.port = port

        def ip_port(self):
            return (self.ip, self.port)

    def __init__(self, huskar_client, service_name, cluster,
                 *args, **kwds):
        super(HuskarClientPool, self).__init__(*args, **kwds)
        self.service_consumer = huskar_client.service_consumer
        self.server_list = []
        self.service_consumer.register_hook_funcation(service_name, cluster,
                                                      self.update_servers,
                                                      trigger=True)
        self.index = random.randint(0, len(self.server_list) - 1)
        thread.start_new_thread(self.maintain_connections, tuple())

    def update_servers(self, servers):
        server_list = []
        for server in servers.values():
            instance = self.parse_server(server)
            if instance:
                server_list.append(instance)
        self.server_list = server_list

    def get_servers_list(self):
        return self.server_list

    def parse_server(self, server):
        try:
            ip = server['ip']
            port = server['port']['main']
        except KeyError:
            logger.error('Error format of server info: {}'.format(server))
            return None
        else:
            return self.ServerInstance(ip, port)

    def yield_server(self):
        assert len(self.server_list) > 0
        if self.index >= len(self.server_list):
            self.index = 0
        instance = self.server_list[self.index]
        self.index += 1
        return instance.ip_port()

    def connection_ok(self, conn):
        if (conn.host, conn.port) in [instance.ip_port() for instance
           in self.server_list] and conn.test_connection():
            return True
        else:
            return False

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

                if self.connection_ok(conn):
                    self.put_back_connection(conn)
                else:
                    conn.close()
