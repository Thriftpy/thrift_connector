# -*- coding: utf-8 -*-

import time
import logging

logger = logging.getLogger(__name__)


class ThriftConnectorHook(object):
    def __init__(self, name):
        self.name = name
        self.callbacks = set()

    def register(self, callback, raises=None):
        self.callbacks.add((callback, raises))

    def send(self, *args, **kwds):
        for (callback, raises) in self.callbacks:
            try:
                callback(*args, **kwds)
            except Exception as e:
                if raises and any(isinstance(e, ec) for ec in raises):
                    raise e
                logger.warning(e, exc_info=True)


before_call = ThriftConnectorHook('before_call')
after_call = ThriftConnectorHook('after_call')


def api_call_context(pool, client, api_name):
    def deco(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            before_call.send(pool, client, api_name, start)
            ret = None
            try:
                ret = func(*args, **kwargs)
                return ret
            except Exception as e:
                ret = e
                raise
            finally:
                now = time.time()
                client.incr_use_count()
                client.set_latest_use_time(now)
                cost = now - start
                after_call.send(pool, client, api_name, start, cost, ret)
        return wrapper
    return deco
