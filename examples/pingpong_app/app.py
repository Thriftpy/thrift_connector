#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
import thriftpy2
from thriftpy2.thrift import TProcessor

thrift_service = thriftpy2.load(os.path.join(os.path.dirname(__file__), "pingpong.thrift"), "pingpong_thrift")  # noqa
service = thrift_service.PingService


class PingpongServer(object):
    def ping(self):
        if os.environ.get('about_to_shutdown') == '1':
            raise service.AboutToShutDownException
        return "pong"

    def win(self):
        return "Yes, you win"

    def sleep(self, seconds):
        time.sleep(seconds)
        return 'good morning'


app = TProcessor(service, PingpongServer())
