# -*- coding: utf-8 -*-
from const import PayType
from alipay import AliPay
from wxpay import WXPay


class PayManager:

    @classmethod
    def get_pay_obj(self, pay_type):
        if pay_type == PayType.ALIPAY:
            return AliPay()
        elif pay_type == PayType.WX:
            return WXPay()
        else:
            return None

    @classmethod
    def precreate(cls, pay_type, order_no, price, notify_url, item_info, device_info):
        payobj = cls.get_pay_obj(pay_type)
        if not payobj:
            return {}
        data = payobj.precreate(order_no, price, notify_url, item_info, device_info)
        return data

    @classmethod
    def refund(cls, pay_type, order_no, money):
        payobj = cls.get_pay_obj(pay_type)
        if not payobj:
            return {}
        return payobj.refund(order_no, money)

    @classmethod
    def query_trade(cls, pay_type, order_no):
        payobj = cls.get_pay_obj(pay_type)
        if not payobj:
            return {}
        res = payobj.query_trade(order_no)
        return res
