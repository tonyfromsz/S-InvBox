# -*- coding: utf-8 -*-

import logging
import sys
import fire
import signal
import errno
import eventlet
eventlet.monkey_patch()  # noqa (code before rest of imports)
import autodiscover
import pytest

from nameko.runners import ServiceRunner
from config import config
from log import init_logger
from service.base import BaseService
from service.service import InvboxService

reload(sys)
sys.setdefaultencoding("utf-8")


logger = logging.getLogger(__name__)


class Command(object):

    def runserver(self):

        def _shutdown(signum, frame):
            eventlet.spawn_n(runner.stop)

        runner = ServiceRunner(config=config.to_dict())
        # for service_cls in BaseService.__subclasses__():
        #     runner.add_service(service_cls)
        runner.add_service(InvboxService)

        signal.signal(signal.SIGTERM, _shutdown)
        runner.start()
        runnlet = eventlet.spawn(runner.wait)

        while True:
            try:
                runnlet.wait()
            except OSError as exc:
                if exc.errno == errno.EINTR:
                    continue
                raise
            except KeyboardInterrupt:
                print()
                try:
                    runner.stop()
                except KeyboardInterrupt:
                    print()  # as above
                    runner.kill()
            else:
                break

    def runtests(self):
        pytest.main(['-x', '--log-level=INFO', 'tests'])
        if hasattr(pytest, "invbox_failed"):
            sys.exit(1)

    def init_test_data(self, create=True, drop=True):
        from models import create_tables, drop_tables
        if drop:
            logging.info("drop table ...")
            drop_tables()

        if create:
            logging.info("create table ...")
            create_tables()

        logging.info("init test data ...")
        from tests import init_data
        init_data.init_all()


if __name__ == "__main__":
    autodiscover.autodiscover("./service")
    init_logger(level=config.log_level, path=config.log_path)
    logging.getLogger("peewee").setLevel(getattr(logging, config.log_level.upper()))
    fire.Fire(Command)
