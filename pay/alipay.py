# -*- coding: utf-8 -*-

"""
支付宝接口文档： https://docs.open.alipay.com/194
"""

import base64
import requests
import os
import logging
import ujson as json

from datetime import datetime as dte
from Crypto.Hash import SHA256
from Crypto.Signature import PKCS1_v1_5 as Signature_pkcs1_v1_5
from Crypto.PublicKey import RSA
from const import PayStatus, PAY_EXPIRE_SECONDS

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
ALIPAY_URL = "https://openapi.alipay.com/gateway.do"
APPID = "2017103009619714"


logger = logging.getLogger(__name__)


with open(os.path.join(CUR_DIR, 'rsa_app_pri_key.pem')) as f:
    g_pri_key = RSA.importKey(f.read())

with open(os.path.join(CUR_DIR, 'rsa_ali_pub_key.pem')) as f:
    g_pub_key = RSA.importKey(f.read())


class AliPay(object):
    """
    使用RSA2签名
    """

    def precreate(self, order_no, price, notify_url, item_info, device_info):
        biz_content = {
            "out_trade_no": order_no,
            "total_amount": float(price) / 100,
            "subject": "%s-%s" % (item_info["name"], device_info["no"]),               # 设备地址-商品名字
            "terminal_id": device_info["no"],
            "timeout_express": "%sm" % (PAY_EXPIRE_SECONDS / 60),
        }
        params = self._build_params("alipay.trade.precreate", biz_content,
                                    notify_url=notify_url)
        r = requests.post(ALIPAY_URL, data=params, timeout=15)
        data = r.json()
        info = data.get("alipay_trade_precreate_response", {})
        logger.info("[alipay] precreate %s", data)
        if info.get("code", "") == "10000":
            return {"code_url": info["qr_code"]}
        else:
            logger.info("[alipay]-%s 支付宝订单创建失败", order_no)
            return {}

    def refund(self, order_no, money):
        biz_content = {
            "out_trade_no": order_no,
            "refund_amount": float(money) / 100,
        }
        params = self._build_params("alipay.trade.refund", biz_content)
        r = requests.post(ALIPAY_URL, data=params, timeout=15)
        data = r.json()
        logger.info("[alipay] refund %s", data)
        info = data.get("alipay_trade_refund_response", {})
        if info.get("code", "") == "10000":
            fee = int(float(info["refund_fee"]) * 100)
            return {"refund_money": fee}
        return {}

    def _build_params(self, method, biz_content, notify_url=None):
        """
        生成请求参数
        """
        now = dte.now()
        params = {
            "app_id": APPID,
            "method": method,
            "charset": "utf-8",
            "sign_type": "RSA2",
            "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
            "version": "1.0",
            "biz_content": json.dumps(biz_content),
        }

        if notify_url is not None:
            params["notify_url"] = notify_url

        msg = "&".join(["%s=%s" % (k, params[k]) for k in sorted(params.keys())])
        sign = self._sign(msg)
        params["sign"] = sign
        return params

    def _sign(self, msg):
        """
        签名
        """
        signer = Signature_pkcs1_v1_5.new(g_pri_key)
        digest = SHA256.new()
        digest.update(msg)
        sign = signer.sign(digest)
        signature = base64.b64encode(sign)
        return signature

    def _verify_sign(self, msg, sign):
        """
        验签

        Return:
            `True` or `False`
        """
        verifier = Signature_pkcs1_v1_5.new(g_pub_key)
        digest = SHA256.new()
        digest.update(msg)
        is_verify = verifier.verify(digest, base64.b64decode(sign))
        return is_verify

    def query_trade(self, order_no):
        "交易查询"

        biz_content = {
            "out_trade_no": order_no,
        }
        params = self._build_params("alipay.trade.query", biz_content)
        r = requests.post(ALIPAY_URL, data=params, timeout=15)
        data = r.json()
        info = data.get("alipay_trade_query_response", {})
        if info.get("code", "") != "10000":
            return {}

        buyer = info.get("buyer_user_id", "")
        refund_money = 0
        pay_money = 0
        status = info["trade_status"]
        if status in ["TRADE_SUCCESS", "TRADE_FINISHED"]:
            pay_status = PayStatus.PAIED
            pay_money = int(float(info["total_amount"]) * 100)
        elif status == "TRADE_CLOSED":
            refund_info = self.query_refund(order_no)
            if refund_info:
                pay_status = PayStatus.REFUND
                refund_money = refund_info["refund_money"]
            else:
                pay_status = PayStatus.CLOSED
        else:
            return {}
        return {
            "buyer": buyer,
            "pay_status": pay_status,
            "refund_money": refund_money,
            "pay_money": pay_money,
        }

    def query_refund(self, order_no):
        "退款查询"
        biz_content = {
            "out_trade_no": order_no,
            "out_request_no": order_no,
        }
        params = self._build_params("alipay.trade.fastpay.refund.query", biz_content)
        r = requests.post(ALIPAY_URL, data=params, timeout=15)
        logger.info("[alipay] refund.query %s", r.content)
        data = r.json()

        info = data.get("alipay_trade_fastpay_refund_query_response", {})
        if info.get("code", "") == "10000":
            if "refund_amount" not in info:
                return {}
            fee = int(float(info["refund_amount"]) * 100)
            return {"refund_money": fee}
        return {}
