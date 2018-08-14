# -*- coding: utf-8 -*-

"""
业务层
"""
import logging
import random
import ujson as json

from datetime import datetime as dte
from models import (db, RedeemActivity, Redeem, VoiceActivity, VoiceWord, Order,
                    Device, DeviceCategory, Road, User)
from selector import UserSelectorProxy
from const import (OrderStatus, PayStatus, DELIVER_EXPIRE_SECONDS,
                   WAITING_PAY_EXPIRE_SECONDS, PayType)
from pay.manager import PayManager
from const import PayTypeMsg, RedeemStatus
# from sms.helper import SMSHelper


logger = logging.getLogger(__name__)


class OrderBiz(object):
    """
    订单相关的业务逻辑
    """

    def __init__(self, order_no="", order=None):
        self.order = None
        if order:
            self.order = order
        if order_no:
            self.order = Order.get(Order.no == order_no)

    def refresh_pay_status(self):
        order = self.order
        res = PayManager.query_trade(order.pay_type, order.no)
        if not res or res["pay_status"] == order.pay_status:
            return
        old_status = order.status
        new_pay_status = res["pay_status"]
        if new_pay_status == PayStatus.PAIED and \
                old_status == OrderStatus.CREATED:
            pay_money = res["pay_money"]
            self.pay_success(pay_money, order.pay_type, buyer=res["buyer"])
        elif new_pay_status == PayStatus.CLOSED and \
                old_status == OrderStatus.CREATED:
            self.pay_fail()
        elif new_pay_status == PayStatus.REFUND:
            self.refund_success(res["refund_money"])

    def create(self, road, amount, pay_type):
        """
        创建订单
        """
        price = road.sale_price * amount
        if not price:
            raise Exception("金额异常<price=%s>", price)

        order_no = Order.generate_order_no()
        order = Order.create(
            no=order_no,
            road=road,
            device=road.device,
            item_amount=amount,
            item=road.item,
            price=price,
            pay_money=0,
            pay_type=pay_type,
            status=OrderStatus.CREATED,
            pay_status=PayStatus.UNPAY,
        )
        order.save()
        logger.info("[order](%s) 创建成功", order_no)
        self.order = order

    def pay_success(self, money, pay_type,
                    redeem=None, voice_word=None, buyer=""):
        """
        订单支付成功
        """
        order = self.order
        q = Order.update(pay_money=money,
                         pay_status=PayStatus.PAIED,
                         pay_type=pay_type,
                         status=OrderStatus.DELIVERING,
                         redeem=redeem,
                         voice_word=voice_word,
                         pay_at=dte.now()) \
                 .where(Order.no == order.no)
        q.execute()
        order.reload()
        logger.info("[order](%s) 支付成功 %s:%s", order.no, PayTypeMsg[pay_type], buyer)

        user = (getattr(order, "user", None) or
                getattr(redeem, "user", None) or
                getattr(voice_word, "user", None))
        try:
            biz = UserBiz(buyer=buyer, pay_type=pay_type, user=user)
            biz.buy_success(order)
        except Exception, e:
            logger.error(e)

    def pay_fail(self):
        """
        支付失败
        """
        order = self.order
        logger.info("[order](%s) 支付失败", order.no)

        old_status = order.status
        if old_status != OrderStatus.CREATED:
            return

        qs = Order.update(status=OrderStatus.CLOSED).where(Order.id == order.id)
        qs.execute()
        order.reload()

    def check_pay_timeout(self):
        order = self.order
        order.reload()
        if order.status != OrderStatus.CREATED:
            return

        create_time = (dte.now() - order.created_at).total_seconds()
        if create_time > WAITING_PAY_EXPIRE_SECONDS:
            qs = Order.update(status=OrderStatus.CLOSED) \
                      .where(Order.id == order.id)
            qs.execute()
            logger.info("[order](%s), 支付超时，超时%s秒",
                        order.no, int(create_time))

    def pay_init_fail(self):
        """
        请求第三方支付创建订单失败
        """
        order = self.order
        logger.info("[order](%s) 支付初始化失败", order.no)

        qs = Order.update(status=OrderStatus.CLOSED).where(Order.id == order.id)
        qs.execute()
        order.reload()

    def pay_init_ok(self):
        """
        请求第三方支付创建订单成功
        """
        order = self.order
        logger.info("[order](%s) 支付初始化成功", order.no)

    def deliver_success(self):
        """
        出货成功
        """
        order = self.order
        q = Order.update(status=OrderStatus.DONE,
                         deliver_at=dte.now()).where(Order.id == order.id)
        q.execute()
        order.reload()
        logger.info("[order](%s) 出货成功", order.no)

        try:
            device_biz = DeviceBiz(device=order.device)
            device_biz.decr_road_stock(order.road)
        except Exception:
            logger.exception("decrease stock error")

    def deliver_fail(self):
        """
        出货失败
        """
        order = self.order
        q = Order.update(status=OrderStatus.DELIVER_FAILED,
                         deliver_at=dte.now()).where(Order.id == order.id)
        q.execute()
        order.reload()
        logger.info("[order](%s) 出货失败", order.no)

        self.refund()

    def check_deliver_timeout(self):
        order = self.order
        order.reload()

        if order.status != OrderStatus.DELIVERING:
            return

        paid_time = (dte.now() - order.pay_at).total_seconds()
        if paid_time > DELIVER_EXPIRE_SECONDS:
            qs = Order.update(status=OrderStatus.DELIVER_TIMEOUT) \
                      .where(Order.id == order.id)
            qs.execute()
            order.reload()
            logger.info("[order]-%s, 出货超时，超时%s秒",
                        order.no, int(paid_time))
            self.refund()

    def refund(self):
        """
        发起退款
        """
        order = self.order
        order.reload()
        if order.status not in [OrderStatus.DELIVER_FAILED,
                                OrderStatus.DELIVER_TIMEOUT]:
            return

        res = {}
        if order.pay_type == PayType.REDEEM:
            biz = MarktingBiz()
            flag = biz.revert_redeem(order.redeem)
            if flag:
                res = {"refund_money": 0}
        elif order.pay_type == PayType.VOICE:
            biz = MarktingBiz()
            flag = biz.revert_voice_code(order.voice_word)
            if flag:
                res = {"refund_money": 0}
        else:
            res = PayManager.refund(order.pay_type, order.no, order.pay_money)

        if res:
            qs = Order.update(status=OrderStatus.REFUNDED,
                              pay_status=PayStatus.REFUND,
                              refund_money=res["refund_money"]) \
                      .where(Order.id == order.id)
            qs.execute()
            order.reload()
            logger.info("[order](%s) 发起退款成功", order.no)
        else:
            logger.info("[order](%s) 发起退款失败", order.no)

    def refund_success(self, money):
        """
        退款成功
        """
        order = self.order
        logger.info("[order](%s) 退款成功", order.no)
        if order.status == OrderStatus.REFUNDED:
            return
        qs = Order.update(status=OrderStatus.REFUNDED,
                          pay_status=PayStatus.REFUND,
                          refund_money=money) \
                  .where(Order.id == order.id)
        qs.execute()
        order.reload()


class DeviceBiz(object):

    def __init__(self, device_no="", device=None):
        if not device and device_no:
            device = Device.get(Device.no == device_no)
        self.device = device

    def get_road_upper_limit(self, no):
        "获取货道商品数量上限"
        cat = self.device.category
        if not hasattr(self, "road_meta_info"):
            self.road_meta_info = json.loads(cat.road_meta_list)
        return self.road_meta_info[int(no) - 1]["upper_limit"]

    def get_road_lower_limit(self, no):
        "获取货道商品数量警报值"
        cat = self.device.category
        if not hasattr(self, "road_meta_info"):
            self.road_meta_info = json.loads(cat.road_meta_list)
        return self.road_meta_info[int(no) - 1]["lower_limit"]

    def choose_device_category(self, device_no):
        if len(device_no) < 11:
            sid = device_no
        else:
            # sid = device_no[6: 10]
            sid = device_no[:8]
        return DeviceCategory.get_or_none(name=sid)

    @db.atomic()
    def create(self, **kwargs):
        """
        新建设备
        """
        assert self.device is None

        if "category" not in kwargs:
            cat = self.choose_device_category(kwargs["no"])
            if not cat:
                logger.error("[device](%s) 未找到对应型号", kwargs["no"])
                raise Exception("未找到对应型号")
            kwargs["category"] = cat

        if "name" not in kwargs:
            kwargs["name"] = kwargs["no"]

        category = kwargs["category"]

        device = Device.create(**kwargs)
        device.save()

        for i in range(category.road_count):
            road = Road.create(
                device=device,
                no="%02d" % (i + 1),
            )
            road.save()
        self.device = device
        logger.info("[device](%s) 创建设备并初始化货道", device.no)
        return device

    def get_available_road(self, item):
        """
        获取此设备能够卖item的货道
        """
        roads = Road.select().where(Road.device == self.device,
                                    Road.item == item)
        if not roads:
            return None

        road = max(roads, key=lambda b: b.amount)
        if road.amount < 0:
            return None
        return road

    def decr_road_stock(self, road):
        """
        减少库存
        """
        device = self.device
        old_stock = road.amount
        if old_stock < 1:
            return

        qs = Road.update(amount=Road.amount - 1) \
                 .where(Road.id == road)
        qs.execute()
        road.reload()

        logger.info("[device](%s-%s) 库存减少1个, 当前库存%s", device.no, road.no, road.amount)

        self.check_stockout(trigger=road)

    def incr_road_stock(self, road, amount=1):
        """
        增加库存
        """
        biz = DeviceBiz(device=road.device)
        limit = biz.get_road_upper_limit(road.no)
        device = self.device
        final = min(limit, road.amount + amount)
        qs = Road.update(amount=final).where(Road.id == road)
        qs.execute()
        road.reload()
        logger.info("[device](%s-%s) 库存增加%s个，当前库存%s", device.no, road.no, amount, road.amount)

        self.check_stockout(trigger=road)

    def check_stockout(self, trigger=None):
        """
        检查设备缺货情况
        """
        device = self.device
        category = device.category
        meta_list = json.loads(category.road_meta_list)

        is_stockout = False
        for road in device.road_set:
            meta_info = meta_list[int(road.no) - 1]
            if road.amount <= meta_info["lower_limit"]:
                is_stockout = True
                break

        if trigger:
            meta_info = meta_list[int(trigger.no) - 1]
            if trigger.amount <= meta_info["lower_limit"]:
                helper = SMSHelper()
                helper.send_lack_warning(device)

        stockout_at = dte.now()
        if is_stockout == device.is_stockout:
            return

        qs = Device.update(is_stockout=is_stockout,
                           stockout_at=stockout_at) \
                   .where(Device.id == device.id)
        qs.execute()

        if is_stockout:
            logger.info("[device](%s) 标记为缺货", device.no)
        else:
            logger.info("[device](%s) 取消缺货标记", device.no)


class MarktingBiz(object):

    def __init__(self):
        self.redeem_error = ""

    def check_get_redeem(self, code):
        """
        获取redeem对象，并检查兑换码有效性
        """
        now = dte.now()
        redeem = Redeem.get_or_none(Redeem.code == code)
        if not redeem:
            self.redeem_error = "错误兑换码"
            return

        if redeem.status != RedeemStatus.UNUSE:
            self.redeem_error = "兑换码已经使用"
            return

        avt = redeem.activity
        print now, avt.valid_end_at, avt.valid_start_at
        if now > avt.valid_end_at or now < avt.valid_start_at:
            self.redeem_error = "无效兑换码"
            return

        return redeem

    def revert_redeem(self, redeem):
        """
        归还兑换码
        """
        if redeem.status != RedeemStatus.USED:
            return False
        q = Redeem.update(status=RedeemStatus.UNUSE,
                          device=None,
                          use_at=None).where(Redeem.id == redeem.id)
        q.execute()
        logger.info("[markting] 归还兑换码成功 <Redeem:%s>", redeem.code)
        return True

    def cost_redeem(self, device, redeem):
        """
        消耗兑换码
        """
        if redeem.status == RedeemStatus.USED:
            return False
        q = Redeem.update(status=RedeemStatus.USED,
                          device=device,
                          use_at=dte.now()).where(Redeem.id == redeem.id)
        q.execute()
        logger.info("[markting] 扣除兑换码成功 <Redeem:%s>", redeem.code)
        return True

    def check_get_voice_activity(self, code):
        """
        获取有效兑换口令对象
        """
        now = dte.now()
        activity = VoiceActivity.get_or_none(VoiceActivity.code == code,
                                             VoiceActivity.valid_end_at >= now,
                                             VoiceActivity.valid_start_at <= now)
        return activity

    def cost_voice_code(self, user, device, activity):
        """
        增加口令兑换
        """
        voice_word = VoiceWord.create(activity=activity,
                                      user=user,
                                      status=RedeemStatus.USED,
                                      device=device,
                                      use_at=dte.now())
        voice_word.save()
        logger.info("[markting] 扣除口令成功 <VoiceWord:%s>", activity.code)
        return True

    def revert_voice_code(self, voiceword):
        """收回口令"""
        activity = voiceword.activity
        voiceword.delete()
        logger.info("[markting] 收回口令成功 <VoiceWord:%s>", activity.code)

    @db.atomic()
    def create_redeem_activity(self, name, usergroup, start_at, end_at, item):
        """
        创建兑换活动，并初始化兑换码
        """
        def _get_random_codes(num):
            # NOTE 目测达到7位数时会比较慢，待优化
            redeem_count = Redeem.select().count()
            if redeem_count + num < 10**5:
                candidates = set(["%05d" % x for x in range(0, 10**5)])
            elif redeem_count + num < 10**6:
                candidates = set(["%06d" % x for x in range(0, 10**6)])
            elif redeem_count + num < 10**7:
                candidates = set(["%07d" % x for x in range(0, 10**7)])

            candidates -= set([t[0] for t in Redeem.select(Redeem.code).tuples()])
            candidates = list(candidates)
            return [candidates.pop(random.randint(0, len(candidates) - 1))
                    for i in range(num)]

        at = RedeemActivity.create(name=name,
                                   valid_start_at=start_at,
                                   valid_end_at=end_at,
                                   item=item,
                                   user_group=usergroup)
        at.save()

        query = json.loads(at.user_group.condition)
        users = UserSelectorProxy(query).select()

        # 给用户分配兑换码
        codes = _get_random_codes(len(users))
        for i, u in enumerate(users):
            if not u.mobile:
                continue

            r = Redeem.create(
                code=codes[i],
                activity=at,
                user=u,
            )
            r.save()

            helper = SMSHelper()
            helper.send_redeem_message(r)

        logger.info("[markting] 创建兑换活动(%s)，分配兑换码%s个", name, len(users))
        return at

    @db.atomic()
    def create_voice_activity(self, code, devicegroup, start_at, end_at,
                              limit, item):
        """
        创建口令活动，及其附带业务逻辑
        """
        at = VoiceActivity.create(code=code,
                                  valid_start_at=start_at,
                                  valid_end_at=end_at,
                                  item=item,
                                  limit=limit,
                                  device_group=devicegroup)
        at.save()
        return at


class UserBiz(object):

    def __init__(self, user=None, buyer="", pay_type=0):
        self.user = None
        if user:
            self.user = user

        if buyer:
            if pay_type == PayType.ALIPAY:
                self.user = User.get_or_create(username=buyer,
                                               aliuserid=buyer)
            elif pay_type == PayType.WX:
                u = User.get_or_none(wxuserid=buyer)
                if not u:
                    u = User.create(username=buyer,
                                    wxuserid=buyer)
                    u.save()
                self.user = u
            else:
                self.user = User.get_or_create(username=buyer)

    def buy_success(self, order):
        "购买成功，出货成功"
        user = self.user
        now = dte.now()
        if not user.first_buy_at:
            user.first_buy_at = now
        user.last_buy_at = now
        user.save()

        order.user = user
        order.save()
