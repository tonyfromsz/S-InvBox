# -*- coding: utf-8 -*-

from models import User, Item, ItemBrand, ItemCategory, Device, Order, Road
from models import Redeem, Supplyer, AddressType, RedeemActivity, VoiceActivity
from models import VoiceWord, SupplyList, DayDeviceStat
from datetime import datetime, timedelta
from const import (RoadStatusMsg, FaultMsg, OrderStatusMsg, RedeemStatusMsg,
                   SupplyStatusMsg)

"""
筛选器
"""

MODEL_NAMES = {
    "user": User,
    "item": Item,
    "itembrand": ItemBrand,
    "brand": ItemBrand,
    "itemcategory": ItemCategory,
    "category": ItemCategory,
    "device": Device,
    "order": Order,
    "road": Road,
    "redeem": Redeem,
    "voiceword": VoiceWord,
    "address_type": AddressType,
    "supplyer": Supplyer,
    "supplylist": SupplyList,
    ("redeem", "activity"): RedeemActivity,
    ("voiceword", "activity"): VoiceActivity,
    "daydevicestat": DayDeviceStat
}


class AttributeSelector(object):
    OPS = []

    def __init__(self, mcls, attr_name, operator, value):
        if operator not in self.OPS:
            raise Exception("operator error")
        self.mcls = mcls
        self.attr_name = attr_name
        self.operator = operator
        self.value = value

    def parse(self):
        raise NotImplemented

    def __str__(self):
        return "{}({}{}{})".format(self.__class__.__name__,
                                   self.attr_name,
                                   self.operator,
                                   self.value)

    @classmethod
    def get_options(self, attr, proxy_name=""):
        return []


class NumberSelector(AttributeSelector):

    OPS = [
        ">",
        "<",
        "=",
        ">=",
        "<=",
        "≠",
    ]
    TYPE_NAME = "number"

    def parse(self):
        attr = getattr(self.mcls, self.attr_name)
        op = self.operator
        if op == ">":
            return attr > self.value
        elif op == "≠":
            return attr != self.value
        elif op == "=":
            return attr == self.value
        elif op == "<":
            return attr < self.value
        elif op == ">=":
            return attr >= self.value
        elif op == "<=":
            return attr <= self.value


class IDSelector(AttributeSelector):

    OPS = [
        "=",
        "≠",
    ]
    TYPE_NAME = "id"

    def parse(self):
        attr = getattr(self.mcls, self.attr_name)
        op = self.operator
        if op == "≠":
            return attr != self.value
        elif op == "=":
            return attr == self.value

    @classmethod
    def get_options(self, attr, proxy_name=""):
        if proxy_name == RoadSelectorProxy.name:
            if attr == "status":
                return [{"key": k, "name": v} for k, v in RoadStatusMsg.items()]
            elif attr == "fault":
                return [{"key": k, "name": v} for k, v in FaultMsg.items()]
        elif proxy_name == OrderSelectorProxy.name:
            if attr == "status":
                return [{"key": k, "name": v} for k, v in OrderStatusMsg.items()]
        elif proxy_name == RedeemSelectorProxy.name:
            if attr == "status":
                return [{"key": k, "name": v} for k, v in RedeemStatusMsg.items()]
        elif proxy_name == VoiceWordSelectorProxy.name:
            if attr == "status":
                return [{"key": k, "name": v} for k, v in RedeemStatusMsg.items()]
        elif proxy_name == SupplyListSelectorProxy.name:
            if attr == "status":
                return [{"key": k, "name": v} for k, v in SupplyStatusMsg.items()]

        tmp = attr.split("__")
        if len(tmp) > 1:
            model_name = tmp[-1]
        else:
            model_name = tmp[0]

        if (proxy_name, model_name) in MODEL_NAMES:
            model_cls = MODEL_NAMES[(proxy_name), model_name]
        else:
            model_cls = MODEL_NAMES[model_name]

        qs = model_cls.select()
        return [{"key": obj.key, "name": obj.name} for obj in qs]


class StringSelector(AttributeSelector):

    OPS = [
        "是",
        "包含",
        "不是",
        "不包含",
        "开头是",
        "结尾是",
        "开头不是",
        "结尾不是"
    ]
    TYPE_NAME = "string"

    def parse(self):
        attr = getattr(self.mcls, self.attr_name)
        op = self.operator
        if op == "是":
            return attr == self.value
        elif op == "不是":
            return attr != self.value
        elif op == "包含":
            return attr.contains(self.value)
        elif op == "不包含":
            return ~attr.contains(self.value)
        elif op == "开头是":
            return attr.startswith(self.value)
        elif op == "结尾是":
            return attr.endswith(self.value)
        elif op == "开头不是":
            return ~attr.startswith(self.value)
        elif op == "结尾不是":
            return ~attr.endswith(self.value)


class DateSelector(AttributeSelector):

    OPS = [
        "最近",
        "固定时段"
    ]
    TYPE_NAME = "date"

    def parse(self):
        attr = getattr(self.mcls, self.attr_name)
        op = self.operator

        if op == "最近":
            start_date = datetime.now() - timedelta(days=int(self.value))
            return attr >= start_date
        elif op == "固定时段":
            start_date = datetime.strptime(self.value[0], "%Y-%m-%d")
            end_date = datetime.strptime(self.value[1], "%Y-%m-%d")
            end_date = end_date + timedelta(days=1)
            return (attr >= start_date) & (attr < end_date)


class BooleanSelector(AttributeSelector):

    OPS = [
        "是",
        "不是"
    ]
    TYPE_NAME = "bool"

    def parse(self):
        assert self.value in [True, False]
        attr = getattr(self.mcls, self.attr_name)
        return attr == self.value

    @classmethod
    def get_options(self, attr, proxy_name=""):
        return {
            True: "是",
            False: "否",
        }


class EventSelector(object):

    pass


class SelectorProxy(object):
    name = ""
    attribute_selectors = {}
    sub_proxies = {}
    ignore_displays = tuple()       # 不在前端显示的Selctor

    def __init__(self, conditions):
        self.conditions = conditions
        self.to_join_models = set()
        self.parse()

    @classmethod
    def get_display_info(cls, name):

        if not SelectorProxy.sub_proxies:
            for subcls in SelectorProxy.__subclasses__():
                SelectorProxy.sub_proxies[subcls.name] = subcls

        selector_cls = SelectorProxy.sub_proxies[name]

        attribute_selectors = []
        ops = {}
        for attribute, (display, selector) in selector_cls.attribute_selectors.items():
            if attribute in selector_cls.ignore_displays:
                continue

            attribute_selectors.append({
                "attribute": attribute,
                "displayName": display,
                "dataType": selector.TYPE_NAME,
                "options": selector.get_options(attribute, proxy_name=name),
            })
            if selector.TYPE_NAME not in ops:
                ops[selector.TYPE_NAME] = selector.OPS

        return {
            "attributeSelectors": attribute_selectors,
            "eventSelectors": [],
            "operators": ops,
        }

    def _parse_single(self, condition):
        if "attribute" in condition:
            attr_name = condition["attribute"]
            if attr_name not in self.attribute_selectors:
                return

            if "value" not in condition or "operator" not in condition:
                return

            trans = self.trans_condition(attr_name, condition["value"])
            if trans:
                return trans

            _, selector_cls = self.attribute_selectors[attr_name]

            attr_split = attr_name.split("__")
            if len(attr_split) == 2:
                model_name, attr_name = tuple(attr_split)
                mcls = MODEL_NAMES[model_name]
                self.to_join_models.add(mcls)
            else:
                mcls = MODEL_NAMES[self.name]

            try:
                selector = selector_cls(mcls,
                                        attr_name,
                                        condition["operator"],
                                        condition["value"])
            except:
                return
            return selector.parse()

        elif "event" in condition:  # 时间帅选器
            pass

    def trans_condition(self, attr, val):
        "条件转换"
        return None

    def parse(self):
        and_lst = []
        for or_conds in self.conditions:
            or_lst = []
            for cond in or_conds:
                tmp = self._parse_single(cond)
                if not tmp:
                    continue
                or_lst.append(tmp)

            if or_lst:
                and_lst.append(reduce(lambda x, y: x | y, or_lst))

        if and_lst:
            where = reduce(lambda x, y: x & y, and_lst)
        else:
            where = None
        self.where_phrase = where

    def select(self):
        qs = MODEL_NAMES[self.name].select()
        for m in self.to_join_models:
            qs = qs.join(m)

        mcls = MODEL_NAMES[self.name]
        if self.where_phrase is not None:
            return qs.where(self.where_phrase).order_by(-getattr(mcls, "id"))
        else:
            return qs.order_by(-getattr(mcls, "id"))


class UserSelectorProxy(SelectorProxy):

    name = "user"
    attribute_selectors = {
        "username": ("用户名", StringSelector),
        "mobile": ("手机号", StringSelector),
        "created_at": ("注册时间", DateSelector)
    }


class AdminSelectorProxy(SelectorProxy):

    name = "admin"
    attribute_selectors = {
        "username": ("用户名", StringSelector),
        "mobile": ("手机号", StringSelector),
        "created_at": ("注册时间", DateSelector)
    }


class ItemSelectorProxy(SelectorProxy):

    name = "item"
    attribute_selectors = {
        "name": ("商品名称", StringSelector),
        "brand": ("商品品牌", IDSelector),
        "category": ("商品分类", IDSelector)
    }


class ItemCategorySelectorProxy(SelectorProxy):

    name = "itemcategory"
    attribute_selectors = {
        "name": ("分类名称", StringSelector),
    }


class ItemBrandSelectorProxy(SelectorProxy):

    mcls = ItemBrand
    name = "itembrand"
    attribute_selectors = {
        "name": ("品牌名称", StringSelector),
    }


class DeviceSelectorProxy(SelectorProxy):

    name = "device"
    attribute_selectors = {
        "involved": ("是否已接入", BooleanSelector),
        "name": ("设备名称", StringSelector),
        "no": ("设备sn", StringSelector),
        "online": ("是否在线", BooleanSelector),
        "address_type": ("投放类型", IDSelector),
        "supplyer": ("配货员", IDSelector),
        "province": ("省份", StringSelector),
        "is_stockout": ("是否缺货", BooleanSelector)
    }

    ignore_displays = ("involved", )

    def trans_condition(self, attr, val):
        if attr == "online":
            if val is True:
                return Device.heartbeat_at > (datetime.now() -
                                              timedelta(seconds=Device.ONLINE_SECONDS))
            else:
                return Device.heartbeat_at <= (datetime.now() -
                                               timedelta(seconds=Device.ONLINE_SECONDS))
        return None


class OrderSelectorProxy(SelectorProxy):

    name = "order"
    attribute_selectors = {
        "item__name": ("商品名称", StringSelector),
        "no": ("订单号", StringSelector),
        "status": ("订单状态", IDSelector),
        "device__province": ("省份", StringSelector),
        "created_at": ("订单时间", DateSelector),
        "device__address_type": ("场地", IDSelector),
        "item": ("商品编号", IDSelector),
        "device": ("设备", IDSelector)
    }


class RoadSelectorProxy(SelectorProxy):

    name = "road"
    attribute_selectors = {
        "item": ("商品", IDSelector),
        "device": ("设备", IDSelector),
        "device__address_type": ("场地", IDSelector),
        "device__supplyer": ("补货员", IDSelector)
        # "device__name": ("设备名", StringSelector),
        # "device__online": ("是否在线", BooleanSelector),
        # "status": ("配货状态", IDSelector),
        # "fault": ("故障状态", IDSelector),
    }


class RedeemSelectorProxy(SelectorProxy):

    name = "redeem"
    attribute_selectors = {
        "user__username": ("用户名", StringSelector),
        "activity": ("活动ID", IDSelector),
        "status": ("兑换状态", IDSelector),
        "device": ("设备ID", IDSelector),
        "use_at": ("兑换日期", DateSelector),
    }


class VoiceWordSelectorProxy(SelectorProxy):

    name = "voiceword"
    attribute_selectors = {
        "user__username": ("用户名", StringSelector),
        "activity": ("活动ID", IDSelector),
        "activity__item": ("商品ID", IDSelector),
        "status": ("兑换状态", IDSelector),
        "device": ("设备ID", IDSelector),
        "use_at": ("兑换日期", DateSelector),
    }


class SupplyListSelectorProxy(SelectorProxy):

    name = "supplylist"
    attribute_selectors = {
        "status": ("配货状态", IDSelector),
    }


class DayDeviceStatProxy(SelectorProxy):

    name = "daydevicestat"
    attribute_selectors = {
        "device": ("设备编号", IDSelector),
        "device__address_type": ("场地", IDSelector),

    }