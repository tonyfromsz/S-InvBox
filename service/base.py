# -*- coding: utf-8 -*-
import logging
import nameko.rpc

logger = logging.getLogger()


class BaseService(object):

    def do_page(self, qs, page, item_parser=None, page_size=10):
        page = max(1, page)
        total_count = qs.count()
        items = []
        for obj in qs.paginate(page, page_size):
            if item_parser:
                items.append(item_parser(obj))
            else:
                items.append(obj)

        return {
            "pageSize": page_size,
            "totalCount": total_count,
            "page": page,
            "items": items,
        }

    def do_export(self, qs, item_parser=None):
        total_count = qs.count()
        items = []
        for obj in qs:
            if item_parser:
                items.append(item_parser(obj))
            else:
                items.append(obj)

        return {
            "totalCount": total_count,
            "items": items
        }


def transaction_rpc(func):
    """
    统一异常处理 & 统一异常日志 & 事务回滚
    """
    from models import db

    @nameko.rpc.rpc
    def wrap(*args, **kwargs):
        with db.atomic() as txn:
            try:
                res = func(*args, **kwargs)
            except Exception:
                logger.exception("execute %s error!", func.__name__)
                txn.rollback()
                return {"resultCode": -1, "resultMsg": "执行失败"}
        return res
    return wrap


def rpc(func):
    """
    统一异常处理 & 统一异常日志
    """

    @nameko.rpc.rpc
    def wrap(*args, **kwargs):
        try:
            res = func(*args, **kwargs)
        except Exception:
            logger.exception("execute %s error!", func.__name__)
            return {"resultCode": -1, "resultMsg": "执行失败"}
        return res
    return wrap
