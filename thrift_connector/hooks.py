# -*- coding: utf-8 -*-

import time
import logging

logger = logging.getLogger(__name__)


class ThriftConnectorHook(object):
    def __init__(self, name):
        self.name = name
        self.callbacks = []

    def register(self, callback):
        self.callbacks.append(callback)

    def send(self, *args, **kwds):
        for c in self.callbacks:
            try:
                c(*args, **kwds)
            except Exception as e:
                logger.warning(e, exc_info=True)


before_call = ThriftConnectorHook('before_call')
after_call = ThriftConnectorHook('after_call')


def api_call_context(pool, client, api_name):
    def deco(func):
        def wrapper(*args, **kwargs):
            now = time.time()
            before_call.send(pool, client, api_name, now)
            ret = None
            try:
                client.incr_use_count()
                ret = func(*args, **kwargs)
                return ret
            except Exception as e:
                ret = e
                raise
            finally:
                cost = time.time() - now
                after_call.send(pool, client, api_name, now, cost, ret)
        return wrapper
    return deco
