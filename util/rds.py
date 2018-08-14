# -*- coding: utf-8 -*-

import redis
from config import config


_redis_pool = None


def get_redis():
    global _redis_pool
    conf = config.redis

    if not _redis_pool:
        _redis_pool = redis.ConnectionPool(
            host=conf["host"],
            port=conf["port"],
        )
    r = redis.Redis(connection_pool=_redis_pool, db=conf["db"])
    return r


class RedisKeys(object):

    LOGIN_SMSCODE = "invbox:smslogin:%s"  # %s表示手机号码, 存验证码

    WECHAT_SMSCODE = "invbox:smswechat:%s"  # %s表示手机号码, 存验证码

    TIMER_LOCK = "invbox:timerlock:%s"    # %s表示被装饰函数名字

    CRON_LOCK = "invbox:cronlock:%s"      # %s表示被装饰函数名字
