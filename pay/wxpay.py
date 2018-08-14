# -*- coding: utf-8 -*-
"""
文档： https://pay.weixin.qq.com/wiki/doc/api/native.php
"""
import os
import time
import random
import requests
import logging

from datetime import datetime as dte, timedelta
from util import md5
from util import xml_to_dict, dict_to_xml
from const import PayStatus, PAY_EXPIRE_SECONDS


CUR_DIR = os.path.dirname(os.path.abspath(__file__))
APP_ID = "wxe3c5da7e6c5d0558"
MERCHANT_ID = "1508397091"
API_KEY = "2271d200a467cf22ca550a6fb22d8c4f"

CERT_PATH = os.path.join(CUR_DIR, "apiclient_cert.pem")
KEY_PATH = os.path.join(CUR_DIR, "apiclient_key.pem")

logger = logging.getLogger()


class WXPay(object):

    def __init__(self):
        pass

    def check_cert(self):
        """
        验证证书
        当返回结果return_code为“SUCCESS”，说明当前客服端已支持DigCert证书
        """
        url = "https://apitest.mch.weixin.qq.com/sandboxnew/pay/getsignkey"
        params = self._build_params({})
        xml_str = dict_to_xml(params)
        r = requests.post(url, data=xml_str, timeout=15)
        data = xml_to_dict(r.content)
        print data

    # 统一下单接口
    def precreate(self, order_no, price, notify_url, item_info, device_info):
        url = "https://api.mch.weixin.qq.com/pay/unifiedorder"
        expire = PAY_EXPIRE_SECONDS
        now = dte.now()
        time_start = now.strftime("%Y%m%d%H%M%S")
        time_expire = (now + timedelta(seconds=expire)).strftime("%Y%m%d%H%M%S")

        biz_content = {
            "device_info": device_info["no"],
            "body": "%s-%s" % (item_info["name"], device_info["no"]),   # 设备地址-商品名字
            'out_trade_no': order_no.encode('utf-8'),                   # 必须utf-8编码
            "fee_type": "CNY",
            'total_fee': price,                                         # 单位是分
            "spbill_create_ip": "127.0.0.1",                    # 客户端IP
            "time_start": time_start,                           # 格式为yyyyMMddHHmmss
            "time_expire": time_expire,
            "notify_url": notify_url,
            "trade_type": "NATIVE",
            "product_id": item_info["id"],                              # 商品ID，扫码支付必须传
        }

        params = self._build_params(biz_content)
        xml_str = dict_to_xml(params)
        r = requests.post(url, data=xml_str, timeout=15)
        data = xml_to_dict(r.content)

        if data["return_code"] != "SUCCESS":
            logger.error("[wxpay] precreate fail. %s", data)
            return {}
        return {
            "prepay_id": data["prepay_id"],
            "code_url": data["code_url"],
        }

    def refund(self, order_no, money):
        biz_content = {
            "out_trade_no": order_no,
            "out_refund_no": order_no,
            "total_fee": money,
            "refund_fee": money,
        }
        params = self._build_params(biz_content)
        xml_str = dict_to_xml(params)
        url = "https://api.mch.weixin.qq.com/secapi/pay/refund"
        r = requests.post(url, data=xml_str, timeout=15, cert=(CERT_PATH, KEY_PATH))
        data = xml_to_dict(r.content)

        if data["return_code"] != "SUCCESS":
            return {}
        return {"refund_money": data["refund_fee"]}

    def _build_params(self, biz_content):
        """
        生成请求参数
        """
        params = {
            "appid": APP_ID,
            'mch_id': MERCHANT_ID,
            'nonce_str': str(int(time.time() * 1000)) + str(random.randint(100, 999)),
            "sign_type": "MD5",
        }
        params.update(biz_content)

        # 签名
        to_sign_str = "&".join(["%s=%s" % (k, params[k]) for k in sorted(params.keys())])
        to_sign_str += "&key=%s" % API_KEY
        params["sign"] = md5(to_sign_str.encode("utf-8")).upper()
        return params

    def query_trade(self, order_no):
        biz_content = {
            "out_trade_no": order_no,
        }
        params = self._build_params(biz_content)
        xml_str = dict_to_xml(params)
        url = "https://api.mch.weixin.qq.com/pay/orderquery"
        r = requests.post(url, data=xml_str, timeout=15)
        data = xml_to_dict(r.content)

        if data["return_code"] != "SUCCESS":
            return {}

        buyer = data.get("openid", "")
        refund_money = 0
        pay_money = 0
        # trade_state = data["trade_state"]
        trade_state = data.get("trade_state", "")
        if trade_state == "SUCCESS":
            pay_status = PayStatus.PAIED
            pay_money = int(data["total_fee"])
        elif trade_state in ["CLOSED", "REVOKED"]:
            pay_status = PayStatus.CLOSED
        elif trade_state == "REFUND":
            pay_status = PayStatus.REFUND
            refund_info = self.query_refund(order_no)
            refund_money = refund_info["refund_money"]
        else:
            return {}

        return {
            "pay_status": pay_status,
            "refund_money": refund_money,
            "pay_money": pay_money,
            "buyer": buyer
        }

    def query_refund(self, order_no):
        biz_content = {
            "out_trade_no": order_no,
        }
        params = self._build_params(biz_content)
        xml_str = dict_to_xml(params)
        url = "https://api.mch.weixin.qq.com/pay/refundquery"
        r = requests.post(url, data=xml_str, timeout=15)
        data = xml_to_dict(r.content)

        if data["return_code"] != "SUCCESS":
            return {}
        fee = int(data["refund_fee_0"])
        return {"refund_money": fee}


if __name__ == "__main__":
    wx = WXPay()
    wx.check_cert()
