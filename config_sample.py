# -*- coding: utf-8 -*-
import os


class Config(object):

    DB_HOST = "172.18.224.101"

    AMQP_URI = 'pyamqp://rbt:rbt123@%s:5673' % DB_HOST
    WEB_SERVER_ADDRESS = '0.0.0.0:8888'
    rpc_exchange = 'nameko-rpc'
    max_workers = 10
    parent_calls_tracked = 10

    mysql = {
        "database": "invbox",
        "host": DB_HOST,
        "port": 3307,
        "user": "root",
        "password": "mysql@yfh",
    }

    redis = {
        "host": DB_HOST,
        "port": 6380,
        "db": 0,
    }

    log_level = "INFO"
    log_path = "/src/logs"

    def to_dict(self):
        data = {}
        cls = self.__class__
        for attr in cls.__dict__:
            if str(attr).startswith("_") or callable(attr):
                continue
            data[attr] = getattr(self, attr)
        return data

    @property
    def database(self):
        if not hasattr(self, "_database"):
            self._database = os.environ.get("DATABASE", "mysql").lower()
        return self._database


config = Config()
