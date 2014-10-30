# -*- coding: utf-8 -*-

import logging
import datetime
import contextlib

logger = logging.getLogger(__name__)


class ThriftBaseClient(object):

    def __init__(self, transport, protocol, client, keepalive=None,
                 pool_generation=0):
        self.transport = transport
        self.protocol = protocol
        self.client = client
        self.alive_until = datetime.datetime.now() + \
            datetime.timedelta(seconds=keepalive) if keepalive else None
        self.pool_generation = pool_generation

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
                pool_generation=0):
        SOCKET = cls.get_socket_factory()(host, port)
        PROTO_FACTORY = cls.get_protoco_factory()
        TRANS_FACTORY = cls.get_transport_factory()

        transport = TRANS_FACTORY(SOCKET)
        protocol = PROTO_FACTORY(transport)

        transport.open()

        return cls(
            transport=transport,
            protocol=protocol,
            client=cls.get_tclient(service, protocol),
            keepalive=keepalive,
            pool_generation=pool_generation
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

    @classmethod
    def get_tclient(self, service, protocol):
        return service.Client(protocol)

    @classmethod
    def get_socket_factory(self):
        from thrift.transport import TSocket
        return TSocket.TSocket


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

    @classmethod
    def get_tclient(self, service, protocol):
        from thriftpy.thrift import TClient
        return TClient(service, protocol)

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

    @classmethod
    def get_tclient(self, service, protocol):
        from thriftpy.thrift import TClient
        return TClient(service, protocol)

    @classmethod
    def get_socket_factory(self):
        from thriftpy.transport import TSocket
        return TSocket


class ClientPool(object):
    def __init__(self, service, host, port, timeout=30, name=None,
                 raise_empty=False, max_conn=30, connction_class=ThriftClient,
                 keepalive=None):
        self.service = service
        self.host = host
        self.port = port
        self.timeout = timeout
        self.name = name or service.__name__
        self.connections = set()
        self.raise_empty = raise_empty
        self.max_conn = max_conn
        self.connction_class = connction_class
        self.keepalive = keepalive
        self.generation = 0

    def keys(self):
        return set([self.name, self.service.__name__])

    def __repr__(self):
        return "<%s service=%r>" % (
            self.__class__.__name__,
            self.keys()
            )

    def clear(self):
        old_connections = self.connections
        self.connections = set()
        self.generation += 1

        for c in old_connections:
            c.close()

    def set_service_uri(self, host, port):
        self.host = host
        self.port = port

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
        else:
            conn.close()

    def produce_client(self):
        return self.connction_class.connect(
            self.service,
            self.host,
            self.port,
            self.timeout,
            keepalive=self.keepalive,
            pool_generation=self.generation
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
