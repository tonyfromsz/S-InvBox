# -*- coding: utf-8 -*-

import alisms
import const as C
import random
import logging
import json
import time

from models import SMSHistory
from util import thread_call_out

LOGIN_VALID_CODE = "SMS_123672353"
SUPPLY_NOTIFY_CODE = "SMS_133180240"
LACK_ITEM_WARNING = "SMS_133155829"
FINSH_SUPPLY_CODE = "SMS_133155255"
REDEEM_CREATE_CODE = "SMS_133155266"

logger = logging.getLogger(__name__)


class SMSHelper(object):

    def _send_sms(self, mobile, tpl_code, data):
        obj = SMSHistory(status=C.SMSStatus.READY,
                         mobile=mobile,
                         tplcode=tpl_code,
                         tplparam=json.dumps(data),
                         channel=C.SMSChannel.ALI)
        obj.save()

        res = alisms.send_sms(obj.id, mobile, "阿西莫夫", tpl_code,
                              template_param=json.dumps(data))
        res = json.loads(res)

        logger.info("[alisms](SendSMS) mobile:%s tpl_code:%s tpl_param:%s response:%s",
                    mobile, tpl_code, data, res)

        if res["Code"].upper() == "OK":
            obj.status = C.SMSStatus.OK
            obj.biz_id = res["BizId"]
        else:
            obj.status = C.SMSStatus.FAIL
        obj.save()
        return obj

    @staticmethod
    def refresh(smsobj, sleep=0):
        if not isinstance(smsobj, SMSHistory):
            smsobj = SMSHistory.get_or_none(id=smsobj)

        if not smsobj:
            return

        if sleep:
            time.sleep(sleep)

        sdate = smsobj.created_at.strftime("%Y%m%d")
        res = alisms.query_send_detail(smsobj.biz_id, smsobj.mobile, 10, 1, sdate)
        logger.info("[alisms](QuerySendDetails) mobile:%s bizid:%s response:%s",
                    smsobj.mobile, smsobj.biz_id, res)

        res = json.loads(res)
        detail = res["SmsSendDetailDTOs"]["SmsSendDetailDTO"]
        if not detail:
            return

        detail = detail[0]
        smsobj.content = detail["Content"]
        smsobj.save()

    def send_login_message(self, mobile):
        "发送登录验证短信"

        code = "%06d" % random.randrange(100001, 999999)
        smsobj = self._send_sms(mobile, LOGIN_VALID_CODE, {"code": code})
        thread_call_out(SMSHelper.refresh, smsobj.id, sleep=10)
        return smsobj

    def send_supply_message(self, device, supplylist):
        "配货通知"
        supplyer = device.supplyer
        params = {
            "no": supplylist.no,
            "device": device.name,
            "address": device.address,
            "region": "%s %s %s" % (device.province, device.city, device.district)
        }

        smsobj = self._send_sms(supplyer.mobile, SUPPLY_NOTIFY_CODE, params)
        thread_call_out(SMSHelper.refresh, smsobj.id, sleep=10)
        return smsobj

    def send_lack_warning(self, device):
        "缺货报警"

        supplyer = device.supplyer
        category = device.category
        meta_list = json.loads(category.road_meta_list)

        lack_items = []
        for road in device.road_set:
            meta_info = meta_list[int(road.no) - 1]
            if road.amount <= meta_info["lower_limit"]:
                lack_items.append(road.item)

        if not lack_items:
            return

        item_names = "，".join([obj.name for obj in lack_items])

        address = device.address
        region = "%s %s %s" % (device.province, device.city, device.district)

        params = {
            "address": "".join([region, address]),
            "device": device.name,
            "item": item_names
        }

        smsobj = self._send_sms(supplyer.mobile, LACK_ITEM_WARNING, params)
        thread_call_out(SMSHelper.refresh, smsobj.id, sleep=10)
        return smsobj

    def send_finish_supply_message(self, supplylist):
        supplyer = supplylist.device.supplyer

        params = {
            "no": supplylist.no,
        }

        smsobj = self._send_sms(supplyer.mobile, FINSH_SUPPLY_CODE, params)
        thread_call_out(SMSHelper.refresh, smsobj.id, sleep=10)
        return smsobj

    def send_redeem_message(self, redeem):
        "TODO 改成异步"
        params = {
            "user": redeem.user.mobile,
            "code": redeem.code,
            "item": redeem.activity.item.name,
        }

        smsobj = self._send_sms(redeem.user.mobile, REDEEM_CREATE_CODE, params)
        thread_call_out(SMSHelper.refresh, smsobj.id, sleep=10)
        return smsobj


if __name__ == "__main__":
    helper = SMSHelper()
    obj = helper.send_login_message("13064754229")
    helper.refresh(SMSHistory.get_or_none(id=3))
