# -*- coding: utf-8 -*-

import logging
import datetime
import contextlib
import random
import thread
import time

logger = logging.getLogger(__name__)


class ThriftConnection(object):
    def __init__(self, protocol, transport, pool_generation, keepalive):
        self.protocol = protocol
        self.transport = transport
        self.pool_generation = pool_generation
        self.alive_until = datetime.datetime.now() + \
            datetime.timedelta(seconds=keepalive) if keepalive else None

    def is_expired(self):
        return self.alive_until and datetime.datetime.now() > self.alive_until

    def test_connection(self):
        return False if self.is_expired or not self.transport.isOpen() else True

    def close(self):
        try:
            self.transport.close()
        except Exception as e:
            logger.warn("Connection close failed: %r" % e)


class ThriftBaseClient(object):
    def __init__(self, host, port, transport, protocol, service,
                 keepalive=None, pool_generation=0, tracking=False,
                 tracker_factory=None, connection=None):
        self.host = host
        self.port = port
        self.transport = transport
        self.protocol = protocol
        self.service = service
        self.pool_generation = pool_generation
        self.tracking = tracking
        self.tracker_factory = tracker_factory

        if not connection:
            self._connection = ThriftConnection(
                self.protocol,
                self.transport,
                self.pool_generation,
                keepalive)
        else:
            self._connection = connection
            self.transport = connection.transport
            self.protocol = connection.protocol

        self.client = self.get_tclient(service, protocol)

    def __repr__(self):
        return "<%s service=%s>" % (
            self.__class__.__name__,
            self.client.__class__.__module__
            )

    def __getattr__(self, name):
        return getattr(self.client, name)

    def close(self):
        self._connection.close()

    def is_expired(self):
        self._connection.is_expired()

    def get_connection(self):
        return self._connection

    def test_connection(self):
        self._connection.test_connection()

    @property
    def alive_until(self):
        return self._connection.alive_until

    @classmethod
    def connect(cls, service, host, port, timeout=30, keepalive=None,
                pool_generation=0, tracking=False, tracker_factory=None):
        SOCKET = cls.get_socket_factory()(host, port)
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
            tracker_factory=tracker_factory
            )

    @property
    def TTransportException(self):
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


class ThriftClientMux(ThriftClient):
    @classmethod
    def get_protoco_factory(self):
        from thrift.protocol import TBinaryProtocol
        return TBinaryProtocol.TBinaryProtocolAccelerated

    @classmethod
    def get_transport_factory(self):
        from thrift.transport import TTransport
        return TTransport.TFramedTransport

    @classmethod
    def connect(cls, svc_name, service, host, port, timeout=30, keepalive=None,
                pool_generation=0, tracking=False, tracker_factory=None):
        SOCKET = cls.get_socket_factory()(host, port)
        PROTO_FACTORY = cls.get_protoco_factory()
        TRANS_FACTORY = cls.get_transport_factory()

        transport = TRANS_FACTORY(SOCKET)
        protocol = PROTO_FACTORY(transport)

        from thrift.protocol import TMultiplexedProtocol
        protocol = TMultiplexedProtocol.TMultiplexedProtocol(protocol, svc_name)

        transport.open()

        return cls(host=host,
                   port=port,
                   transport=transport,
                   protocol=protocol,
                   service=service,
                   keepalive=keepalive,
                   pool_generation=pool_generation,
                   tracking=tracking,
                   tracker_factory=tracker_factory)


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
            from thriftpy.thrift import TTrackedClient
            client = TTrackedClient(self.tracker_factory, service, protocol)
        else:
            from thriftpy.thrift import TClient
            client = TClient(service, protocol)
        return client

    @classmethod
    def get_socket_factory(self):
        from thriftpy.transport import TSocket
        return TSocket


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
            from thriftpy.thrift import TTrackedClient
            client = TTrackedClient(self.tracker_factory, service, protocol)
        else:
            from thriftpy.thrift import TClient
            client = TClient(service, protocol)
        return client

    @classmethod
    def get_socket_factory(self):
        from thriftpy.transport import TSocket
        return TSocket


class BaseClientPool(object):
    def __init__(self, service, timeout=30, name=None,
                 raise_empty=False, max_conn=30, connection_class=ThriftClient,
                 keepalive=None, tracking=False, tracker_factory=None):
        self.service = service
        self.timeout = timeout
        self.name = name or service.__name__
        self.connections = set()
        self.raise_empty = raise_empty
        self.max_conn = max_conn
        self.connection_class = connection_class
        self.keepalive = keepalive
        self.generation = 0
        self.tracking = tracking
        self.tracker_factory = tracker_factory

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
        if not self.connections:
            if self.raise_empty:
                raise self.Empty
            return

        connection = self.connections.pop()
        if connection.test_connection():  # make sure old connection is usable
            return connection
        else:
            connection.close()

    def put_back_connection(self, conn):
        assert isinstance(conn, ThriftBaseClient)
        if self.max_conn > 0 and len(self.connections) < self.max_conn and\
                conn.pool_generation == self.generation:
            self.connections.add(conn)
            return True
        else:
            conn.close()
            return False

    def produce_client(self):
        host, port = self.yield_server()
        return self.connection_class.connect(
            self.service,
            host,
            port,
            self.timeout,
            keepalive=self.keepalive,
            pool_generation=self.generation,
            tracking=self.tracking,
            tracker_factory=self.tracker_factory,
            )

    def get_client(self):
        return self.get_client_from_pool() or self.produce_client()

    def __getattr__(self, name):
        def call(*args, **kwds):
            client = self.get_client()
            api = getattr(client, name, None)
            try:
                if api and callable(api):
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


class ClientPool(BaseClientPool):
    def __init__(self, service, host, port, timeout=30, name=None,
                 raise_empty=False, max_conn=30, connection_class=ThriftClient,
                 keepalive=None, tracking=False, tracker_factory=None):
        super(ClientPool, self).__init__(
            service=service,
            timeout=timeout,
            name=name,
            raise_empty=raise_empty,
            max_conn=max_conn,
            connection_class=connection_class,
            keepalive=keepalive,
            tracking=tracking,
            tracker_factory=tracker_factory,
            )
        self.host = host
        self.port = port

    def set_servers(self, server_info):
        host, port = server_info
        self.host = host
        self.port = port

    def yield_server(self):
        return self.host, self.port


class BaseClientPoolMux(BaseClientPool):
    def __init__(self, services, timeout=30, raise_empty=False, max_conn=30,
                 connection_class=ThriftClient, keepalive=None, tracking=False,
                 tracker_factory=None):

        self.services = services
        self.timeout = timeout
        self.connections = set()
        self.raise_empty = raise_empty
        self.max_conn = max_conn
        self.connection_class = connection_class
        self.keepalive = keepalive
        self.generation = 0
        self.tracking = tracking
        self.tracker_factory = tracker_factory

    def keys(self):
        return [(svc_name, svc.__name__) for svc_name, svc in self.services.items()]

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

    def get_connection_from_pool(self):
        if not self.connections:
            if self.raise_empty:
                raise self.Empty
            return

        connection = self.connections.pop()
        if connection.test_connection():  # make sure old connection is usable
            return connection
        else:
            connection.close()

    def put_back_connection(self, connection):
        if (self.max_conn > 0 and len(self.connections) < self.max_conn
            and connection.pool_generation == self.generation):

            self.connections.add(connection)
            return True

        connection.close()
        return False

    def produce_client(self, svc_name):
        host, port = self.yield_server()
        return self.connection_class.connect(
            svc_name,
            self.services.get(svc_name),
            host,
            port,
            self.timeout,
            keepalive=self.keepalive,
            pool_generation=self.generation,
            tracking=self.tracking,
            tracker_factory=self.tracker_factory)

    def get_client(self, svc_name):
        connection = self.get_connection_from_pool()
        if not connection:
            return self.produce_client(svc_name)

        host, port = self.yield_server()
        return self.connection_class(
            host=host,
            port=port,
            protocol=None,
            transport=None,
            service=self.services.get(svc_name),
            keepalive=self.keepalive,
            pool_generation=self.generation,
            tracking=self.tracking,
            tracker_factory=self.tracker_factory,
            connection=connection)

    def __getattr__(self, name):
        def call(svc_name, *args, **kwds):
            client = self.get_client(svc_name)
            api = getattr(client, name, None)
            try:
                if api and callable(api):
                    return api(*args, **kwds)
                raise AttributeError("%s not found in %s" % (name, client))
            finally:
                self.put_back_connection(client.get_connection())
        return call

    @contextlib.contextmanager
    def connection_ctx(self, svc_name):
        client = self.get_client(svc_name)
        try:
            yield client
            self.put_back_connection(client.get_connection())
        except client.TTransportException:
            client.close()
            raise
        except Exception:
            self.put_back_connection(client.get_connection())
            raise


class ClientPoolMux(BaseClientPoolMux):
    def __init__(self, services, host, port, timeout=30, raise_empty=False,
                 max_conn=30, connection_class=ThriftClient, keepalive=None,
                 tracking=False, tracker_factory=None):

        super(ClientPoolMux, self).__init__(
            services=services,
            timeout=timeout,
            raise_empty=raise_empty,
            max_conn=max_conn,
            connection_class=connection_class,
            keepalive=keepalive,
            tracking=tracking,
            tracker_factory=tracker_factory)

        self.host = host
        self.port = port

    def set_servers(self, server_info):
        host, port = server_info
        self.host = host
        self.port = port

    def yield_server(self):
        return self.host, self.port


class HeartbeatClientPool(ClientPool):

    def __init__(self, service, host, port, timeout=30, name=None,
                 raise_empty=False, max_conn=30, connection_class=ThriftClient,
                 keepalive=None, tracking=False, tracker_factory=None):
        super(HeartbeatClientPool, self).__init__(
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
            tracker_factory=tracker_factory
            )
        self.host = host
        self.port = port
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
        # override, do not test connection before returning client
        if not self.connections:
            if self.raise_empty:
                raise self.Empty
            return

        connection = self.connections.pop()
        return connection

    def maintain_connections(self):
        while True:
            time.sleep(max([1, self.timeout - 1]))

            if self.pool_size() == 0:
                # do not check when pool is empty
                return

            iter_list = [x for x in self.connections]

            for client in iter_list:
                if client.is_expired():
                    self._close_and_remove_client(client)
                    continue

                try:
                    client.ping()
                except Exception as e:
                    logger.warn(
                        'Error sending heartbeat: %s, %s',
                        self.service.__name__, e
                        )
                    self._close_and_remove_client(client)
                    continue


class MultiServerClientBase(ClientPool):
    def __init__(self, service, servers, timeout=30, name=None,
                 raise_empty=False, max_conn=30, connection_class=ThriftClient,
                 keepalive=None, tracking=False, tracker_factory=None):
        super(MultiServerClientBase, self).__init__(
            service=service,
            timeout=timeout,
            name=name,
            raise_empty=raise_empty,
            max_conn=max_conn,
            connection_class=connection_class,
            keepalive=keepalive,
            tracking=tracking,
            tracker_factory=None,
            )

        self.servers = servers

    def set_servers(self, server_info):
        for i in server_info:
            assert len(i) == 2
        self.servers = server_info


class RandomMultiServerClient(MultiServerClientBase):
    def yield_server(self):
        assert len(self.servers) > 0
        return random.choice(self.servers)


class RoundRobinMultiServerClient(MultiServerClientBase):
    def __init__(self, *args, **kwds):
        super(RoundRobinMultiServerClient, self).__init__(*args, **kwds)
        self.index = 0
        self.servers = list(self.servers)
        random.shuffle(self.servers)

    def yield_server(self):
        assert len(self.servers) > 0
        if self.index >= len(self.servers):
            self.index = 0
        ret = self.servers[self.index]
        self.index += 1
        return ret
