# -*- coding: utf-8 -*-

import time
import eventlet
import random
import re

from datetime import datetime as dte
from logging import getLogger
from nameko.extensions import Entrypoint
from util.rds import get_redis, RedisKeys


logger = getLogger(__name__)


class DistributedTimer(Entrypoint):
    """
    跟官方timer不同的是，DistributedTimer每次interval只有集群中的一个节点会运行。
    """

    def __init__(self, interval):

        self.interval = interval
        self.should_stop = False
        self.gt = None

    def start(self):
        logger.debug('starting %s', self)
        self.gt = self.container.spawn_managed_thread(self._run)

    def stop(self):
        logger.debug('stopping %s', self)
        self.should_stop = True
        self.gt.wait()

    def kill(self):
        logger.debug('killing %s', self)
        self.gt.kill()

    def _run(self):
        while True:
            rds = get_redis()
            t = time.time()
            key = RedisKeys.TIMER_LOCK % self.method_name
            ok = rds.set(key, t, nx=True, ex=self.interval)
            if ok:
                self.handle_timer_tick()

            if self.should_stop:
                break

            eventlet.sleep(random.randint(1, 5) * 0.1)

    def handle_timer_tick(self):
        args = ()
        kwargs = {}

        self.container.spawn_worker(self, args, kwargs)


class DistributedCron(Entrypoint):
    """
    任务调度
    """

    def __init__(self, trigger_time):
        """
        trigger_time: 触发时间，格式必须是 xxxx-yy-zz aa:bb:cc

        例子：
            @distributed_cron("****-**-** 00:01:00")
            def run_if_new_day():
                "每天凌晨00:01执行"
                pass

            @distributed_cron("2018-06-01 08:00:00")
            def run_if_new_day():
                "18年六一8点准时执行"
                pass
        """

        reg = "[\d|\*]{4}-[\d|\*]{2}-[\d|\*]{2} [\d|\*]{2}:[\d|\*]{2}:[\d|\*]{2}"
        pattern = re.compile(reg)
        if not pattern.match(trigger_time):
            raise Exception("trigger_time's format is illegal")

        self.trigger_time = trigger_time
        self.should_stop = False
        self.gt = None

    def start(self):
        logger.debug('starting %s', self)
        self.gt = self.container.spawn_managed_thread(self._run)

    def stop(self):
        logger.debug('stopping %s', self)
        self.should_stop = True
        self.gt.wait()

    def kill(self):
        logger.debug('killing %s', self)
        self.gt.kill()

    def _run(self):
        while True:
            if self.should_stop:
                break

            now = dte.now().strftime("%Y-%m-%d %H:%M:%S")
            rds = get_redis()
            t = time.time()
            key = RedisKeys.CRON_LOCK % self.method_name

            if not self._fuzzy_equal(now):
                eventlet.sleep(0.2)
                continue

            ok = rds.set(key, t, nx=True, ex=1)
            if ok:
                self.handle_timer_tick()

            eventlet.sleep(0.2)

    def _fuzzy_equal(self, time):
        if len(time) != len(self.trigger_time):
            return False

        for i in range(len(time)):
            c1, c2 = time[i], self.trigger_time[i]
            if c2 != "*" and c1 != c2:
                return False
        else:
            return True

    def handle_timer_tick(self):
        args = ()
        kwargs = {}

        self.container.spawn_worker(self, args, kwargs)


distributed_timer = DistributedTimer.decorator
distributed_cron = DistributedCron.decorator
