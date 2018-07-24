# -*- coding: utf-8 -*-

import logging
import ujson as json
import selector as slt
import const as C

from datetime import datetime as dte, timedelta
from peewee import fn
from util.rds import get_redis, RedisKeys
from models import (Device, Supplyer, Admin, UserGroup, ApkVersion, Advertisement,
                    Video, Image, ADImage, ADVideo, Item, Road, ItemBrand, Redeem,
                    RedeemActivity, ItemCategory, VoiceActivity, Order,
                    AddressType, DeviceCategory, DeviceGroup, SupplyList,
                    DayItemStat, DayDeviceStat, DayUserGroupStat, DayStat)
from util import md5, xml_to_dict
from base import BaseService, rpc, transaction_rpc
from selector import (UserSelectorProxy, SelectorProxy, ItemSelectorProxy,
                      ItemBrandSelectorProxy, ItemCategorySelectorProxy,
                      OrderSelectorProxy, DeviceSelectorProxy, RoadSelectorProxy,
                      RedeemSelectorProxy, VoiceWordSelectorProxy, AdminSelectorProxy)
from const import (OrderStatus, PayStatus, PayType, SupplyStatus, RedeemStatus, RoadStatus)
from pay.manager import PayManager
from biz import OrderBiz, DeviceBiz, MarktingBiz
from sms.helper import SMSHelper
from entrypoint import distributed_timer

logger = logging.getLogger()


class InvboxService(BaseService):

    name = "invbox"

    @distributed_timer(interval=20)
    def cluster_heartbeat(self):
        "每20秒触发一次执行"
        logger.info("[cluster_heartbeat]")

        for o in Order.select().where(Order.status.not_in(
                [OrderStatus.DONE, OrderStatus.REFUNDED, OrderStatus.CLOSED])):
            biz = OrderBiz(order=o)
            biz.refresh_pay_status()

        for o in Order.select().where(Order.status == OrderStatus.CREATED):
            biz = OrderBiz(order=o)
            biz.check_pay_timeout()

        for o in Order.select().where(Order.status == OrderStatus.DELIVERING):
            biz = OrderBiz(order=o)
            biz.check_deliver_timeout()

    @rpc
    def check_login(self, username, password):
        admin = Admin.get_or_none((Admin.username == username) |
                                  (Admin.mobile == username))
        if not admin or admin.password != md5(password):
            return {
                "resultCode": 1,
                "resultMsg": "用户名或密码错误"
            }
        res = {
            "resultCode": 0,
            "resultMsg": "OK",
        }
        res.update(admin.to_dict())
        return res

    @rpc
    def check_login_with_sms(self, mobile, code):
        """
        短信验证码登录
        """
        admin = Admin.get_or_none(mobile=mobile)
        if not admin:
            return {
                "resultCode": 1,
                "resultMsg": "此手机号未注册"
            }
        key = RedisKeys.LOGIN_SMSCODE % mobile
        rds = get_redis()
        if code != rds.get(key):
            return {
                "resultCode": 1,
                "resultMsg": "验证码错误"
            }

        res = {
            "resultCode": 0,
            "resultMsg": "OK",
        }
        res.update(admin.to_dict())
        return res

    @rpc
    def get_admin(self, _id):
        admin = Admin.get_or_none(Admin.id == _id)
        if not admin:
            return {
                "resultCode": 1,
                "resultMsg": "用户不存在"
            }
        res = {
            "resultCode": 0,
            "resultMsg": "OK",
        }
        res.update(admin.to_dict())
        return res

    @rpc
    def add_admin(self, username, mobile, role):
        if Admin.get_or_none(username=username):
            return {
                "resultCode": 1,
                "resultMsg": "已存在%s" % username
            }

        if Admin.get_or_none(mobile=mobile):
            return {
                "resultCode": 1,
                "resultMsg": "已存在%s" % mobile
            }

        admin = Admin.create(username=username,
                             mobile=mobile,
                             role=role)
        admin.save()
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(admin.to_dict())
        return data

    @rpc
    def get_admins(self, page=1, page_size=10, query=[]):
        return self.do_page(
            AdminSelectorProxy(query).select(),
            page,
            item_parser=Admin.to_dict,
            page_size=page_size,
        )

    @rpc
    def send_login_message(self, mobile):
        if not Admin.get_or_none(mobile=mobile):
            return {
                "resultCode": 1,
                "resultMsg": "用户不存在"
            }
        expire_seconds = 5 * 60

        key = RedisKeys.LOGIN_SMSCODE % mobile
        rds = get_redis()
        left_time = rds.ttl(key)
        if left_time and expire_seconds - int(left_time) < 60:
            return {
                "resultCode": 1,
                "resultMsg": "发送失败，1分钟内只能发送一次验证短信"
            }

        helper = SMSHelper()
        smsobj = helper.send_login_message(mobile)

        if smsobj.status == C.SMSStatus.OK:
            info = json.loads(smsobj.tplparam)
            code = info["code"]
            rds.set(key, code, ex=expire_seconds)
            return {
                "resultCode": 0,
                "resultMsg": "发送成功"
            }
        else:
            return {
                "resultCode": 1,
                "resultMsg": "短信发送失败"
            }

    @rpc
    def get_supplyers(self, ids=[], page=1, page_size=10):
        conditions = None
        if ids:
            conditions = Supplyer.id.in_(ids)

        return self.do_page(
            Supplyer.select().where(conditions),
            page,
            item_parser=Supplyer.to_dict,
            page_size=page_size,
        )

    @rpc
    def add_supplyer(self, name, phone):
        if Supplyer.select().where(Supplyer.mobile == phone).count():
            return {
                "resultCode": 1,
                "resultMsg": "已存在补货员%s" % phone
            }

        supplyer = Supplyer.create(name=name, mobile=phone)
        supplyer.save()
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(supplyer.to_dict())
        return data

    @transaction_rpc
    def modify_supplyers(self, info_list):
        """
        批量修改

        只要一个修改失败，则全部回滚。

        info_list:
            [
                {
                    "id": "",
                    "name": "",
                    "mobile": "",
                }
            ]
        """
        for d in info_list:
            phone = d["mobile"]

            check = Supplyer.get_or_none(mobile=d["mobile"])
            if check and check.id != d["id"]:
                return {
                    "resultCode": 1,
                    "resultMsg": "已存在补货员%s" % phone
                }

            q = Supplyer.update(name=d["name"], mobile=d["mobile"]) \
                        .where(Supplyer.id == d["id"])
            q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "修改成功"
        }

    @transaction_rpc
    def delete_supplyers(self, ids):
        if Device.select().filter(Device.supplyer.in_(ids)).count() > 0:
            return {
                "resultCode": 1,
                "resultMsg": "有设备引用此补货员，无法删除"
            }

        q = Supplyer.delete().where(Supplyer.id.in_(ids))
        q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @rpc
    def get_users(self, page=1, page_size=10, query={}):

        def _parser(obj):
            return {
                "id": obj.id,
                "username": obj.username,
                "mobile": obj.mobile,
                "birthday": obj.birthday.strftime("%Y-%m-%d %H:%M:%S")
                                if obj.birthday else "",
                "wxuserid": obj.wxuserid,
                "aliuserid": obj.aliuserid,
                "age": 0,
                "firstBuyAt": obj.first_buy_at.strftime("%Y-%m-%d %H:%M:%S")
                                if obj.first_buy_at else "",
                "lastBuyAt": obj.last_buy_at.strftime("%Y-%m-%d %H:%M:%S")
                                if obj.last_buy_at else "",
                "buyCount": Order.select().where(Order.user == obj, Order.status == OrderStatus.DONE).count(),
                "buyCountOf28Days": Order.select().where(Order.user == obj,
                                                         Order.created_at >= dte.now() - timedelta(days=28),
                                                         Order.status == OrderStatus.DONE).count(),
                "redeemTotal": obj.redeem_set.count(),
                "redeemUsed": obj.redeem_set.where(Redeem.status == RedeemStatus.USED).count(),
                "createdAt": obj.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }

        return self.do_page(
            UserSelectorProxy(query).select(),
            page,
            item_parser=_parser,
            page_size=page_size,
        )

    @rpc
    def get_user_groups(self, page=1, page_size=10):
        return self.do_page(
            UserGroup.select(),
            page,
            item_parser=UserGroup.to_dict,
            page_size=page_size,
        )

    @rpc
    def add_user_group(self, name, condition):
        name = name.strip()
        if not name:
            return {
                "resultCode": 1,
                "resultMsg": "名称不能为空"
            }

        check = UserGroup.get_or_none(name=name)
        if check:
            return {
                "resultCode": 1,
                "resultMsg": "已存在用户群 %s" % name
            }

        obj = UserGroup.create(name=name,
                               condition=json.dumps(condition))
        obj.save()
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(obj.to_dict())
        return data

    @transaction_rpc
    def modify_user_groups(self, info_list):
        for d in info_list:
            name = d["name"]
            if not name.strip():
                return {
                    "resultCode": 1,
                    "resultMsg": "名称不能为空"
                }

            check = UserGroup.get_or_none(name=d["name"])
            if check and check.id != d["id"]:
                return {
                    "resultCode": 1,
                    "resultMsg": "已存在用户群 %s" % d["name"]
                }

            q = UserGroup.update(name=d["name"],
                                 condition=json.dumps(d["condition"])) \
                         .where(UserGroup.id == d["id"])
            q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "修改成功"
        }

    @transaction_rpc
    def delete_user_groups(self, ids):
        q = UserGroup.delete().where(UserGroup.id.in_(ids))
        q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @rpc
    def get_newest_apk(self):
        qs = ApkVersion.select().order_by(ApkVersion.version)
        if not qs:
            return {}
        apk = qs[-1]
        return {
            "version": apk.version,
            "downloadUrl": apk.url,
        }

    @rpc
    def get_device_ads(self, device_id):
        device = Device.get_or_create(no=device_id)

        da = Advertisement.get_or_none(Advertisement.device == device)
        if not da:
            return {}

        return {
            "aText": da.a_text,
            "aVideos": [
                {
                    "videoUrl": da.a_video1.video_url,
                    "md5": da.a_video1.md5,
                },
                {
                    "videoUrl": da.a_video2.video_url,
                    "md5": da.a_video2.md5,
                },
                {
                    "videoUrl": da.a_video3.video_url,
                    "md5": da.a_video3.md5,
                },
                {
                    "videoUrl": da.a_video4.video_url,
                    "md5": da.a_video4.md5,
                },
            ],
            "bImages": [
                {
                    "imageUrl": da.b_image1.image_url,
                    "md5": da.b_image1.md5,
                }
            ],
            "cImages": [
                {
                    "imageUrl": da.c_image1.image_url,
                    "md5": da.c_image1.md5,
                }
            ]
        }

    @rpc
    def check_add_image(self, md5, url, base_url=""):
        """
        md5已存在，则不重复加入
        """
        image = Image.get_or_none(Image.md5 == md5)
        if not image:
            image = Image.create(
                md5=md5,
                url=url,
            )
            image.save()
        return image.to_dict(base_url=base_url)

    @rpc
    def get_adimages(self, page=1, base_url=""):
        return self.do_page(
            ADImage.select(),
            page,
            item_parser=lambda obj: ADImage.to_dict(obj, base_url=base_url),
        )

    @rpc
    def add_adimage(self, name, image, base_url=""):
        at = ADImage.create(name=name, image=image)
        at.save()
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(at.to_dict(base_url=base_url))
        return data

    @transaction_rpc
    def modify_adimages(self, info_list):
        for d in info_list:
            q = ADImage.update(name=d["name"], image=d["image"]) \
                       .where(ADImage.id == d["id"])
            q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "修改成功"
        }

    @transaction_rpc
    def delete_adimages(self, ids):
        q = ADImage.delete().where(ADImage.id.in_(ids))
        q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @rpc
    def get_advideos(self, page=1, base_url=""):
        return self.do_page(
            ADVideo.select(),
            page,
            item_parser=lambda obj: ADVideo.to_dict(obj, base_url=base_url),
        )

    @rpc
    def add_advideo(self, name, video, base_url=""):
        at = ADVideo.create(name=name, video=video)
        at.save()
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(at.to_dict(base_url=base_url))
        return data

    @transaction_rpc
    def modify_advideos(self, info_list):
        for d in info_list:
            q = ADVideo.update(name=d["name"], video=d["video"]) \
                       .where(ADVideo.id == d["id"])
            q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "修改成功"
        }

    @transaction_rpc
    def delete_advideos(self, ids):
        q = ADVideo.delete().where(ADVideo.id.in_(ids))
        q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @rpc
    def get_ads(self, device=None, page=1, base_url=""):
        if device:
            qs = Advertisement.select().where(Advertisement.device == device)
        else:
            qs = Advertisement.select()

        return self.do_page(
            qs,
            page,
            item_parser=lambda obj: Advertisement.to_dict(obj, base_url=base_url),
        )

    @transaction_rpc
    def modify_ads(self, info_list):
        for d in info_list:
            device = d["device"]

            adobj = Advertisement.get_or_none(Advertisement.device == device)
            if not adobj:
                adobj = Advertisement.create(device=device, a_text="")
                adobj.save()

            adobj.a_video1 = d["aVideo1"]
            adobj.a_video2 = d["aVideo2"]
            adobj.a_video3 = d["aVideo3"]
            adobj.a_video4 = d["aVideo4"]
            adobj.a_text = d["aText"] or ""
            adobj.b_image1 = d["bImage1"]
            adobj.c_image1 = d["cImage1"]
            adobj.save()
        return {
            "resultCode": 0,
            "resultMsg": "修改成功"
        }

    @rpc
    def get_video_detail(self, md5):
        video = Video.get_or_none(Video.md5 == md5)
        if not video:
            return
        return video.to_dict()

    @rpc
    def add_video(self, md5, url):
        video = Video.create(md5=md5, url=url)
        video.save()
        return video.to_dict()

    @rpc
    def get_selectors(self, name):
        res = SelectorProxy.get_display_info(name)
        return res

    @rpc
    def get_categories_for_device(self, device_id, base_url=""):
        "获取类别"
        device = Device.get_or_create(no=device_id)

        data = {}
        for obj in Road.select().where(Road.device == device):
            item = obj.item
            if not item:
                continue

            if item.category_id not in data:
                category = item.category
                data[item.category_id] = {
                    "id": category.id,
                    "name": category.name,
                    "thumbnail": base_url + category.thumbnail_url,
                    "imageUrl": base_url + category.image_url,
                    "description": u"查看详情",
                    "items": {}
                }

            path = ""
            if item.thumbnails:
                path = item.thumbnails[0].url

            item_data = data[item.category_id]["items"]
            if item.id not in item_data:
                info = {
                    "id": item.id,
                    "road": obj.no,
                    "name": item.name,
                    "thumbnail": base_url + path,
                    "description": item.description,
                    "price": obj.price or item.basic_price,
                    "stock": obj.amount
                }
                item_data[item.id] = info
            else:
                old = item_data[item.id]
                old["stock"] = max(old["stock"], obj.amount)

        result = data.values()
        for d in result:
            items = d["items"].values()
            items.sort(key=lambda d: d["road"])
            d["items"] = items
        return result

    @rpc
    def exchange_item_by_redeem(self, device_id, code):
        biz_markting = MarktingBiz()
        redeem = biz_markting.check_get_redeem(code)
        if not redeem:
            return {
                "resultCode": 1,
                "resultMsg": biz_markting.redeem_error or "无效兑换码"
            }

        # 走创建订单流程
        activity = redeem.activity
        item = activity.item
        biz_device = DeviceBiz(device_id)
        road = biz_device.get_available_road(item)
        if not road:
            return {
                "resultCode": 1,
                "resultMsg": "太热销啦，正在加速补货中，为您带来不便请多多见谅！"
            }

        biz_order = OrderBiz()
        biz_order.create(road, 1, PayType.REDEEM)

        # 走支付流程
        is_ok = biz_markting.cost_redeem(biz_device.device, redeem)
        if not is_ok:
            biz_order.on_pay_fail()
            return {
                "resultCode": 1,
                "resultMsg": "兑换码扣除失败"
            }

        biz_order.pay_success(0, PayType.REDEEM, redeem=redeem)

        order = biz_order.order
        return {
            "resultCode": 0,
            "resultMsg": "兑换成功",
            "orderNo": order.no,
            "orderStatus": order.status,
            "payAt": order.pay_at.strftime("%Y-%m-%d %H:%M:%S"),
            "itemId": order.item_id,
            "itemAmount": order.item_amount,
            "payMoney": order.pay_money,
            "deviceBoxNo": road.no,
        }

    @rpc
    def exchange_item_by_voice(self, device_id, code, user_id):
        biz_markting = MarktingBiz()
        activity = biz_markting.check_get_voice_activity(code)
        if not activity:
            return {
                "resultCode": 1,
                "resultMsg": "无效口令"
            }

        # if device not in activity.device_group.devices:
        #     return {
        #         "resultCode": 2,
        #         "resultMsg": "非常抱歉，此口令不能在该台设备使用"
        #     }

        item = activity.item
        biz_device = DeviceBiz(device_id)
        road = biz_device.get_available_road(item)
        if not road:
            return {
                "resultCode": 1,
                "resultMsg": "太热销啦，正在加速补货中，为您带来不便请多多见谅！"
            }

        # 走创建订单流程
        biz_order = OrderBiz()
        biz_order.create(road, 1, PayType.VOICE)

        # 走支付流程
        is_ok = biz_markting.cost_voice_code(user_id, biz_device.device, activity)
        if not is_ok:
            biz_order.on_pay_fail()
            return {
                "resultCode": 1,
                "resultMsg": "兑换口令扣除失败"
            }

        order = biz_order.order
        return {
            "resultCode": 0,
            "resultMsg": "兑换成功",
            "orderNo": order.no,
            "orderStatus": order.status,
            "payAt": order.pay_at.strftime("%Y-%m-%d %H:%M:%S"),
            "itemId": order.item_id,
            "itemAmount": order.item_amount,
            "payMoney": order.pay_money,
            "deviceBoxNo": road.no,
        }

    @rpc
    def get_item_categories(self, page=1, page_size=10, base_url="", query=[]):
        return self.do_page(
            ItemCategorySelectorProxy(query).select(),
            page,
            item_parser=lambda obj: ItemCategory.to_dict(obj, base_url=base_url),
            page_size=page_size,
        )

    @rpc
    def add_category(self, name, thumbnail, image, base_url=""):
        if not name:
            return {
                "resultCode": 1,
                "resultMsg": "名称不能为空"
            }

        if ItemCategory.get_or_none(ItemCategory.name == name):
            return {
                "resultCode": 1,
                "resultMsg": "已存在: %s" % name
            }

        if thumbnail is not None:
            thumbnail = Image.get_or_none(Image.id == thumbnail)
            if not thumbnail:
                return {
                    "resultCode": 2,
                    "resultMsg": "图片不存在"
                }

        if image is not None:
            image = Image.get_or_none(Image.id == image)
            if not image:
                return {
                    "resultCode": 3,
                    "resultMsg": "图片不存在"
                }

        at = ItemCategory.create(name=name,
                                 thumbnail=thumbnail,
                                 image=image)
        at.save()
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(at.to_dict(base_url=base_url))
        return data

    @transaction_rpc
    def modify_categories(self, info_list):
        for d in info_list:
            if not d["name"]:
                return {
                    "resultCode": 1,
                    "resultMsg": "名称不能为空"
                }

            check = ItemCategory.get_or_none(ItemCategory.name == d["name"])
            if check and check.id != d["id"]:
                return {
                    "resultCode": 1,
                    "resultMsg": "已存在%s" % d["name"]
                }
            q = ItemCategory.update(name=d["name"]).where(ItemCategory.id == d["id"])
            q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "修改成功"
        }

    @transaction_rpc
    def delete_categories(self, ids):
        if Item.select().filter(Item.category.in_(ids)).count() > 0:
            return {
                "resultCode": 1,
                "resultMsg": "有商品引用此类型，无法删除"
            }

        q = ItemCategory.delete().where(ItemCategory.id.in_(ids))
        q.execute()

        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @rpc
    def get_brands(self, page=1, page_size=10, query=[]):
        return self.do_page(
            ItemBrandSelectorProxy(query).select(),
            page,
            page_size=page_size,
            item_parser=ItemBrand.to_dict,
        )

    @rpc
    def add_brand(self, name):
        if not name:
            return {
                "resultCode": 1,
                "resultMsg": "名称不能为空"
            }

        if ItemBrand.get_or_none(ItemBrand.name == name):
            return {
                "resultCode": 1,
                "resultMsg": "已存在 %s" % name
            }

        at = ItemBrand.create(name=name)
        at.save()
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(at.to_dict())
        return data

    @transaction_rpc
    def modify_brands(self, info_list):
        for d in info_list:
            if not d["name"].strip():
                return {
                    "resultCode": 1,
                    "resultMsg": "名称不能为空"
                }
            check = ItemBrand.get_or_none(ItemBrand.name == d["name"])
            if check and check.id != d["id"]:
                return {
                    "resultCode": 1,
                    "resultMsg": "已存在 %s" % d["name"]
                }
            q = ItemBrand.update(name=d["name"]).where(ItemBrand.id == d["id"])
            q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "修改成功"
        }

    @transaction_rpc
    def delete_brands(self, ids):
        if Item.select().filter(Item.brand.in_(ids)).count() > 0:
            return {
                "resultCode": 1,
                "resultMsg": "有商品引用此品牌，无法删除"
            }

        q = ItemBrand.delete().where(ItemBrand.id.in_(ids))
        q.execute()

        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @rpc
    def get_items(self, page=1, page_size=10, base_url="", query=[]):
        return self.do_page(
            ItemSelectorProxy(query).select(),
            page,
            page_size=page_size,
            item_parser=lambda obj: Item.to_dict(obj, base_url=base_url),
        )

    @rpc
    def get_items_for_device(self, device_id, base_url=""):
        device = Device.get_or_create(no=device_id)

        items = []
        check = {}
        for obj in Road.select().where(Road.device == device):
            item = obj.item
            if not item or item.id in check:
                continue

            items.append({
                "id": item.id,
                "road": obj.no,
                "name": item.name,
                "thumbnail": base_url + obj.sale_image_url,
                "description": item.description,
                "price": obj.price or item.basic_price,
                "stock": obj.amount,
                "category": item.category.name
            })
            check[item.id] = 1
        items.sort(key=lambda d: d["road"])
        return items

    @transaction_rpc
    def add_item(self, name, no, category, brand,
                 thumbnails, basic_price, cost_price):
        if Item.get_or_none(name=name):
            return {
                "resultCode": 1,
                "resultMsg": "新增失败：已存在 %s" % name,
            }

        at = Item.create(name=name,
                         no=no,
                         category=category,
                         brand=brand,
                         basic_price=basic_price,
                         cost_price=cost_price)
        at.save()
        at.thumbnails.clear()
        at.thumbnails.add(list(set(thumbnails)))
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(at.to_dict())
        return data

    @transaction_rpc
    def modify_items(self, info_list):
        for d in info_list:
            obj = Item.get_or_none(Item.id == d["id"])
            if not obj:
                continue

            tmp = Item.get_or_none(name=d["name"])
            if tmp and tmp.id != obj.id:
                return {
                    "resultCode": 1,
                    "resultMsg": "新增失败：已存在 %s" % d["name"],
                }

            obj.name = d["name"]
            obj.no = d["no"]
            obj.category = d["category"]
            obj.brand = d["brand"]
            obj.basic_price = d["basicPrice"]
            obj.cost_price = d["costPrice"]
            obj.updated_at = dte.now()
            obj.save()

            obj.thumbnails.clear()
            obj.thumbnails.add(list(set(d["thumbnails"])))
        return {
            "resultCode": 0,
            "resultMsg": "修改成功"
        }

    @transaction_rpc
    def delete_items(self, ids):
        q = Item.delete().where(Item.id.in_(ids))
        q.execute()

        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @rpc
    def get_order_detail(self, order_no):
        biz = OrderBiz(order_no=order_no)
        biz.refresh_pay_status()
        order = biz.order

        return {
            "orderNo": order.no,
            "roadNo": order.road.no,
            "device": order.device.no,
            "itemAmount": order.item_amount,
            "item": order.item_id,
            "payMoney": order.pay_money,
            "payStatus": order.pay_status,
            "payType": order.pay_type,
            "payAt": order.pay_at.strftime("%Y-%m-%d %H:%M:%S")
                        if order.pay_at else "",
            "price": order.price,
            "status": order.status,
            "qrcodeUrl": order.qrcode_url,
            "createdAt": order.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        }

    @rpc
    def create_order(self, device_id, item_id, amount, pay_type, notify_url):
        item = Item.get(Item.id == item_id)
        biz_device = DeviceBiz(device_id)

        road = biz_device.get_available_road(item)
        if not road:
            return {
                "resultCode": 1,
                "resultMsg": "太热销啦，正在加速补货中，为您带来不便请多多见谅！"
            }

        biz_order = OrderBiz()
        biz_order.create(road, amount, pay_type)
        order = biz_order.order

        item = order.item
        item_info = {"id": item.id, "name": item.name}
        device = order.device
        device_info = {"id": device.id, "no": device.no}

        data = PayManager.precreate(order.pay_type,
                                    order.no,
                                    order.price,
                                    notify_url,
                                    item_info,
                                    device_info)
        if data:
            biz_order.pay_init_ok()
        else:
            biz_order.pay_init_fail()
            return {
                "resultCode": 1,
                "resultMsg": "支付订单创建失败"
            }

        order.qrcode_url = data["code_url"]
        order.save()
        result = {
            "resultCode": 0,
            "resultMsg": "创建成功",
            "deviceBoxNo": road.no,
            "itemId": item.id,
            "amount": order.item_amount,
            "status": order.status,
            "orderNo": order.no,
            "orderPrice": float(order.price) / 100,
            "qrcodeUrl": data["code_url"],
        }
        return result

    @transaction_rpc
    def deliver_result(self, order_no, is_success):
        biz = OrderBiz(order_no=order_no)
        order = biz.order
        if order.status in [OrderStatus.DONE, OrderStatus.DELIVER_FAILED]:
            logger.error("收到重复的出货请求")
            return

        if is_success:
            biz.deliver_success()
        else:
            biz.deliver_fail()

    @rpc
    def get_orders(self, page=1, base_url="", page_size=10, query=[]):
        def _parser(obj):
            device = obj.device
            road = obj.road
            item = obj.item
            user = obj.user
            d = {
                "id": obj.id,
                "no": obj.no,
                "device": {
                    "id": device.id,
                    "no": device.no,
                    "name": device.name,
                },
                "road": {
                    "id": road.id,
                    "no": road.no
                },
                "item": {
                    "id": item.id,
                    "name": item.name
                },
                "user": {
                    "id": user.id,
                    "mobile": user.mobile,
                    "wxuserid": user.wxuserid,
                } if user else {},
                "status": obj.status,
                "price": obj.price,
                "payMoney": obj.pay_money,
                "payType": obj.pay_type,
                "payAt": obj.pay_at.strftime("%Y-%m-%d %H:%M:%S")
                            if obj.pay_at else "",
                "deliverAt": obj.deliver_at.strftime("%Y-%m-%d %H:%M:%S")
                            if obj.deliver_at else "",
                "createdAt": obj.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
            return d

        return self.do_page(
            OrderSelectorProxy(query).select(),
            page,
            item_parser=_parser,
        )

    @rpc
    def order_overview(self, start_date, end_date, page=1):
        """
        订单概括

        [start_date, end_date]
        """
        start_date = dte.strptime(start_date, "%Y-%m-%d")
        end_date = dte.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

        qs = Device.select(Device,
                           fn.COUNT(Order.id).alias("total_order"),
                           fn.SUM(Order.pay_money).alias("total_income")) \
                   .join(Order) \
                   .group_by(Device) \
                   .where(Order.created_at >= start_date,
                          Order.created_at < end_date,
                          Order.status == OrderStatus.DONE)

        def _parser(obj):
            sub_qs = Order.select().where(Order.device == obj,
                                          Order.status == OrderStatus.DONE,
                                          Order.created_at >= start_date,
                                          Order.created_at < end_date)
            return {
                "device": {
                    "id": obj.id,
                    "address": obj.address,
                    "name": obj.name,
                },
                "totalOrder": obj.total_order,
                "alipayOrder": sub_qs.where(Order.pay_type == PayType.ALIPAY).count(),
                "wxpayOrder": sub_qs.where(Order.pay_type == PayType.WX).count(),
                "redeemOrder": sub_qs.where(Order.pay_type == PayType.REDEEM).count(),
                "totalIncome": int(obj.total_income),
            }

        return self.do_page(
            qs,
            page,
            item_parser=_parser,
        )

    @rpc
    def get_qrcode_url(self, order_no):
        order = Order.get_or_none(Order.no == order_no)
        if not order:
            return {
                "resultCode": 1,
                "resultMsg": "订单(%s)不存在" % order_no
            }
        return {
            "resultCode": 0,
            "resultMsg": "",
            "qrcodeUrl": order.qrcode_url,
        }

    @rpc
    def wxpay_notify(self, xml):
        data = xml_to_dict(xml)
        logger.info("[wxnotify] %s", data)

        if data["return_code"] != "SUCCESS" or data["result_code"] != "SUCCESS":
            return {}

        biz = OrderBiz(data.get("out_trade_no"))
        order = biz.order

        if order.pay_status == PayStatus.PAIED:
            logger.warning("[wxnotify] ignore")
            return {}

        money = int(data["total_fee"])
        biz.pay_success(money, PayType.WX, buyer=data["openid"])

    @rpc
    def alipay_notify(self, content):
        logger.info("[alipaynotify] %s", content)

        data = {}
        for part in content.split("&"):
            k, v = part.split("=")
            data[k] = v.decode("gbk")

        if data["trade_status"] != "TRADE_SUCCESS":
            return {}

        biz = OrderBiz(data.get("out_trade_no"))
        order = biz.order

        if order.pay_status == PayStatus.PAIED:
            logger.warning("[alipaynotify] ignore")
            return {}

        money = int(float(data["total_amount"]) * 100)
        biz.pay_success(money, PayType.ALIPAY, buyer=data["buyer_user_id"])

    @rpc
    def online_heartbeat(self, device_id, client_ip):
        device = Device.get_or_create(no=device_id)
        qs = Device.update(heartbeat_at=dte.now()).where(Device.id == device.id)
        qs.execute()

    @rpc
    def get_involved_devices(self, page=1, page_size=10, query=[]):
        query.append([
            {
                "operator": "是",
                "attribute": "involved",
                "value": True
            }
        ])

        def _item_parser(obj):
            d = {
                "id": obj.id,
                "name": obj.name,
                "address": obj.address,
                "online": obj.online,
                "category": obj.category.to_dict() if obj.category else {},
                "addressType": obj.address_type.to_dict() if obj.address_type else {},
                "supplyer": obj.supplyer.to_dict() if obj.supplyer else {},
                "sn": obj.no,
                "roadCount": obj.category.road_count if obj.category else 0,
                "doorOpened": obj.door_opened,
                "province": obj.province,
                "city": obj.city,
                "isStockout": obj.is_stockout,
                "district": obj.district,
                "createdAt": obj.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "updatedAt": obj.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
            return d
        return self.do_page(DeviceSelectorProxy(query).select(),
                            page,
                            page_size=page_size,
                            item_parser=_item_parser)

    @rpc
    def get_uninvolved_devices(self, page=1, page_size=10, query=[]):
        query.append([
            {
                "operator": "是",
                "attribute": "involved",
                "value": False
            }
        ])

        def _item_parser(obj):
            d = {
                "id": obj.id,
                "category": obj.category.to_dict() if obj.category else {},
                "sn": obj.no,
                "createdAt": obj.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "name": obj.name,
            }
            return d
        return self.do_page(DeviceSelectorProxy(query).select(),
                            page,
                            item_parser=_item_parser,
                            page_size=page_size)

    @rpc
    def get_stockout_devices(self, page=1, page_size=10):
        "获取缺货设备"
        query = []
        query.append([
            {
                "operator": "是",
                "attribute": "is_stockout",
                "value": True
            }
        ])

        def _item_parser(obj):
            d = {
                "id": obj.id,
                "name": obj.name,
                "address": obj.address,
                "stockoutAt": obj.stockout_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
            return d

        return self.do_page(DeviceSelectorProxy(query).select(),
                            page,
                            item_parser=_item_parser,
                            page_size=page_size)

    @rpc
    def involve_device(self, id,
                       name="",
                       province="",
                       city="",
                       district="",
                       address="",
                       address_type_id=0,
                       supplyer_id=0):

        if not name.strip():
            return {
                "resultCode": 1,
                "resultMsg": "设备名不能为空",
            }

        device = Device.get_or_none(Device.id == id)
        if not device:
            return {
                "resultCode": 1,
                "resultMsg": "未找到设备信息",
            }

        check_obj = Device.get_or_none(Device.name == name)
        if check_obj and check_obj.id != device.id:
            return {
                "resultCode": 1,
                "resultMsg": "设备名不能重复",
            }

        supplyer = Supplyer.get_or_none(Supplyer.id == supplyer_id)
        if supplyer_id and not supplyer:
            return {
                "resultCode": 1,
                "resultMsg": "补货员不存在",
            }

        address_type = AddressType.get_or_none(AddressType.id == address_type_id)
        if address_type_id and not address_type:
            return {
                "resultCode": 1,
                "resultMsg": "点位类型不存在",
            }

        q = Device.update(name=name,
                          supplyer=supplyer,
                          involved=True,
                          city=city,
                          address=address,
                          province=province,
                          district=district,
                          updated_at=dte.now(),
                          address_type=address_type).where(Device.id == id)
        q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "接入成功"
        }

    @rpc
    def get_device_categories(self, page=1, page_size=10):
        return self.do_page(DeviceCategory.select(),
                            page,
                            item_parser=DeviceCategory.to_dict,
                            page_size=page_size)

    @rpc
    def add_device_category(self, name, road_list):
        name = name.strip()
        if not name:
            return {
                "resultCode": 1,
                "resultMsg": "名称不能为空"
            }

        if DeviceCategory.get_or_none(name=name):
            return {
                "resultCode": 1,
                "resultMsg": "新增失败：%s已存在" % name,
            }

        road_list = filter(lambda d: d, road_list)
        for d in road_list:
            if "upperLimit" not in d and "lowerLimit" not in d:
                return {
                    "resultCode": 1,
                    "resultMsg": "新增失败：数据格式错误",
                }

        new_road_list = [{
            "upper_limit": int(d["upperLimit"]),
            "lower_limit": int(d["lowerLimit"])
        } for d in road_list]

        at = DeviceCategory.create(
            name=name,
            road_meta_list=json.dumps(new_road_list),
            road_count=len(new_road_list),
        )
        at.save()
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(at.to_dict())
        return data

    @transaction_rpc
    def delete_device_categories(self, ids):
        q = DeviceCategory.delete().where(DeviceCategory.id.in_(ids))
        q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @transaction_rpc
    def modify_device_categories(self, info_list):
        return {
            "resultCode": 1,
            "resultMsg": "不支持操作"
        }

        for d in info_list:
            new_road_list = [{
                "upper_limit": int(i["upperLimit"]),
                "lower_limit": int(i["lowerLimit"])
            } for i in d["roadList"]]

            obj = DeviceCategory.get_or_none(id=d["id"])
            if not obj:
                continue

            if obj.road_count and obj.road_count != len(new_road_list):
                raise Exception("不能修改货道数量")

            tmp = DeviceCategory.get_or_none(name=d["name"])
            if tmp and tmp.id != obj.id:
                return {
                    "resultCode": 1,
                    "resultMsg": "新增失败：已存在 %s" % d["name"],
                }

            obj.name = d["name"]
            obj.road_meta_list = json.dumps(new_road_list)
            obj.road_count = len(new_road_list)
            obj.save()

        return {
            "resultCode": 0,
            "resultMsg": "成功"
        }

    @rpc
    def get_address_types(self, page=1, page_size=10, query=[]):
        return self.do_page(AddressType.select(),
                            page,
                            page_size=page_size,
                            item_parser=AddressType.to_dict)

    @rpc
    def add_address_type(self, name):
        if not name:
            return {
                "resultCode": 1,
                "resultMsg": "名称不能为空",
            }

        if AddressType.get_or_none(AddressType.name == name):
            return {
                "resultCode": 1,
                "resultMsg": "已存在地址类型: %s" % name
            }

        at = AddressType.create(name=name)
        at.save()
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(at.to_dict())
        return data

    @transaction_rpc
    def modify_address_types(self, info_list):
        for d in info_list:
            d["name"] = d["name"].strip()
            if not d["name"]:
                return {
                    "resultCode": 1,
                    "resultMsg": "名称不能为空",
                }
            check = AddressType.get_or_none(name=d["name"])
            if check and check.id != d["id"]:
                return {
                    "resultCode": 1,
                    "resultMsg": "已存在地址类型：%s" % d["name"],
                }
            q = AddressType.update(name=d["name"]).where(AddressType.id == d["id"])
            q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "修改成功"
        }

    @transaction_rpc
    def delete_address_types(self, ids):
        q = Device.update(address_type=None).where(Device.address_type.in_(ids))
        q.execute()

        q = AddressType.delete().where(AddressType.id.in_(ids))
        q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @rpc
    def get_roads(self, page=1, page_size=10, base_url="", query=[]):
        road_meta_data = {}
        for dc in DeviceCategory.select():
            road_meta_data[dc.id] = json.loads(dc.road_meta_list)

        # TODO apply for query
        def _item_parser(obj):
            device = obj.device
            item = obj.item
            road_meta_list = road_meta_data[device.category_id]
            d = {
                "id": obj.id,
                "no": obj.no,
                "device": {
                    "id": device.id,
                    "name": device.name,
                    "online": device.online,
                },
                "item": {
                    "id": item.id,
                    "name": item.name,
                } if item else {},
                "amount": obj.amount,
                "limit": road_meta_list[int(obj.no) - 1]["upper_limit"],
                "status": obj.status_msg,
                "price": obj.price or getattr(item, "basic_price", 0),
                "thumbnails": [o.to_dict(base_url=base_url) for o in obj.thumbnails or getattr(item, "thumbnails", [])],
                "fault": obj.fault_msg,
                "faultAt": obj.fault_at.strftime("%Y-%m-%d %H:%M:%S")
                                if obj.fault_at else "",
                "updatedAt": obj.updated_at.strftime("%Y-%m-%d %H:%M:%S")
            }
            return d
        return self.do_page(RoadSelectorProxy(query).select(),
                            page,
                            item_parser=_item_parser,
                            page_size=page_size)

    @transaction_rpc
    def modify_roads(self, info_list):
        """
        商品信息修改
        """
        for d in info_list:
            obj = Road.get_or_none(Road.id == d["id"])
            obj.price = float(d["price"])
            if obj.price <= 0:
                return {
                    "resultCode": 1,
                    "resultMsg": "售价必须为正数"
                }
            if obj.item_id != d["item"]:    # 说明换货了
                return {
                    "resultCode": 1,
                    "resultMsg": "换货请走配货单流程"
                }
            obj.save()
            obj.thumbnails.clear()
            obj.thumbnails.add(list(set(d["thumbnails"])))

        return {
            "resultCode": 0,
            "resultMsg": "成功"
        }

    @rpc
    def set_road_fault(self, device_id, road_no):
        "设置货道故障"
        device = Device.get_or_none(no=device_id)
        road = Road.get_or_none(device=device, no=road_no)
        if not road:
            return {
                "resultCode": 1,
                "resultMsg": "无此货道"
            }
        road.status = RoadStatus.FAULT
        road.save()
        return {
            "resultCode": 0,
            "resultMsg": "成功"
        }

    @rpc
    def remove_road_fault(self, device_id, road_no):
        "解除货道故障"
        device = Device.get_or_none(no=device_id)
        road = Road.get_or_none(device=device, no=road_no)
        if not road:
            return {
                "resultCode": 1,
                "resultMsg": "无此货道"
            }
        road.status = RoadStatus.SELLING
        road.save()
        return {
            "resultCode": 0,
            "resultMsg": "成功"
        }

    @rpc
    def get_device_groups(self, page=1, page_size=10):
        return self.do_page(
            DeviceGroup.select(),
            page,
            item_parser=DeviceGroup.to_dict,
            page_size=page_size,
        )

    @rpc
    def add_device_group(self, name, condition):
        name = name.strip()
        if not name:
            return {
                "resultCode": 1,
                "resultMsg": "名称不能为空"
            }

        check = DeviceGroup.get_or_none(name=name)
        if check:
            return {
                "resultCode": 1,
                "resultMsg": "已存在设备群 %s" % name
            }
        obj = DeviceGroup.create(name=name,
                                 condition=json.dumps(condition))
        obj.save()
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(obj.to_dict())
        return data

    @transaction_rpc
    def modify_device_groups(self, info_list):
        for d in info_list:
            name = d["name"]
            if not name.strip():
                return {
                    "resultCode": 1,
                    "resultMsg": "名称不能为空"
                }

            check = DeviceGroup.get_or_none(name=d["name"])
            if check and check.id != d["id"]:
                return {
                    "resultCode": 1,
                    "resultMsg": "已存在设备群 %s" % d["name"]
                }

            q = DeviceGroup.update(name=d["name"],
                                   condition=json.dumps(d["condition"])) \
                           .where(DeviceGroup.id == d["id"])
            q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "修改成功"
        }

    @transaction_rpc
    def delete_device_groups(self, ids):
        q = DeviceGroup.delete().where(DeviceGroup.id.in_(ids))
        q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @rpc
    def get_redeem_activities(self, page=1, page_size=10):
        return self.do_page(
            RedeemActivity.select(),
            page,
            item_parser=lambda obj: RedeemActivity.to_dict(obj),
            page_size=page_size,
        )

    @transaction_rpc
    def add_redeem_activity(self,
                            name,
                            user_group,
                            valid_start_at,
                            valid_end_at,
                            item):
        if not name:
            return {
                "resultCode": 1,
                "resultMsg": "名称不能为空"
            }
        check = RedeemActivity.get_or_none(name=name)
        if check:
            return {
                "resultCode": 1,
                "resultMsg": "已存在 %s" % name
            }

        start_at = dte.strptime(valid_start_at, "%Y-%m-%d %H:%M:%S")
        end_at = dte.strptime(valid_end_at, "%Y-%m-%d %H:%M:%S")
        biz = MarktingBiz()
        at = biz.create_redeem_activity(name, user_group, start_at,
                                        end_at, item)
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(at.to_dict())
        return data

    @transaction_rpc
    def modify_redeem_activities(self, info_list):
        for d in info_list:
            name = d["name"].strip()
            if not name:
                return {
                    "resultCode": 1,
                    "resultMsg": "名称不能为空"
                }
            check = RedeemActivity.get_or_none(name == name)
            if check and check.id != d["id"]:
                return {
                    "resultCode": 1,
                    "resultMsg": "已存在 %s" % name
                }

            start_at = dte.strptime(d["validStartAt"], "%Y-%m-%d %H:%M:%S")
            end_at = dte.strptime(d["validEndAt"], "%Y-%m-%d %H:%M:%S")
            q = RedeemActivity.update(
                name=d["name"],
                valid_start_at=start_at,
                valid_end_at=end_at,
                user_group=d["userGroup"],
                item=d["item"]).where(RedeemActivity.id == d["id"])
            q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "修改成功"
        }

    @transaction_rpc
    def delete_redeem_activities(self, ids):
        q = RedeemActivity.delete().where(RedeemActivity.id.in_(ids))
        q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @rpc
    def get_voice_activities(self, page=1, page_size=10):
        return self.do_page(
            VoiceActivity.select(),
            page,
            item_parser=lambda obj: VoiceActivity.to_dict(obj),
            page_size=page_size,
        )

    @transaction_rpc
    def add_voice_activity(self,
                           code,
                           device_group,
                           valid_start_at,
                           valid_end_at,
                           limit,
                           item):
        start_at = dte.strptime(valid_start_at, "%Y-%m-%d %H:%M:%S")
        end_at = dte.strptime(valid_end_at, "%Y-%m-%d %H:%M:%S")
        biz = MarktingBiz()
        at = biz.create_voice_activity(code, device_group, start_at, end_at,
                                       limit, item)
        data = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        data.update(at.to_dict())
        return data

    @transaction_rpc
    def modify_voice_activities(self, info_list):
        for d in info_list:
            start_at = dte.strptime(d["validStartAt"], "%Y-%m-%d %H:%M:%S")
            end_at = dte.strptime(d["validEndAt"], "%Y-%m-%d %H:%M:%S")
            q = VoiceActivity.update(
                code=d["code"],
                valid_start_at=start_at,
                valid_end_at=end_at,
                device_group=d["deviceGroup"],
                limit=d["limit"],
                item=d["item"]).where(VoiceActivity.id == d["id"])
            q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "修改成功"
        }

    @transaction_rpc
    def delete_voice_activities(self, ids):
        q = VoiceActivity.delete().where(VoiceActivity.id.in_(ids))
        q.execute()
        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @rpc
    def get_redeems(self, page=1, page_size=10, query=[]):

        def _serilizer(obj):
            aty = obj.activity
            u = obj.user
            dv = obj.device
            item = aty.item
            return {
                "activity": {
                    "id": aty.id,
                    "name": aty.name,
                },
                "code": obj.code,
                "user": {
                    "id": u.id,
                    "username": u.username,
                    "mobile": u.mobile,
                },
                "device": {
                    "id": dv.id,
                    "name": dv.name,
                    "address": dv.address,
                } if dv else {},
                "item": {
                    "id": item.id,
                    "name": item.name,
                },
                "status": obj.status,
                "useAt": obj.use_at.strftime("%Y-%m-%d %H:%M:%S")
                            if obj.use_at else "",
                "createdAt": obj.created_at.strftime("%Y-%m-%d %H:%M:%S")
            }

        return self.do_page(
            RedeemSelectorProxy(query).select(),
            page,
            item_parser=_serilizer,
            page_size=page_size,
        )

    @rpc
    def get_voice_words(self, page=1, page_size=10, query=[]):

        def _serilizer(obj):
            aty = obj.activity
            u = obj.user
            dv = obj.device
            item = aty.item
            return {
                "activity": {
                    "id": aty.id,
                    "name": aty.name,
                    "code": aty.code,
                },
                "user": {
                    "id": u.id,
                    "username": u.username,
                    "mobile": u.mobile,
                } if u else {},
                "device": {
                    "id": dv.id,
                    "name": dv.name,
                    "address": dv.address,
                } if dv else {},
                "item": {
                    "id": item.id,
                    "name": item.name,
                },
                "status": obj.status,
                "useAt": obj.use_at.strftime("%Y-%m-%d %H:%M:%S")
                    if obj.use_at else "",
            }

        return self.do_page(
            VoiceWordSelectorProxy(query).select(),
            page,
            item_parser=_serilizer,
            page_size=page_size,
        )

    @rpc
    def get_supplylist(self, page=1, page_size=10, query=[]):
        return self.do_page(
            slt.SupplyListSelectorProxy(query).select(),
            page,
            item_parser=SupplyList.to_dict,
            page_size=page_size,
        )

    @transaction_rpc
    def add_supplylist(self, device_id, roads_info):
        device = Device.get_or_none(id=device_id)
        if not device:
            return {
                "resultCode": 1,
                "resultMsg": "设备不存在"
            }

        if not device.supplyer:
            return {
                "resultCode": 1,
                "resultMsg": "该设备还未配置配货员"
            }

        if SupplyList.select().where(
                SupplyList.device == device,
                SupplyList.status == SupplyStatus.DOING).count():
            return {
                "resultCode": 1,
                "resultMsg": "该设备有未完成的配货单"
            }

        data_before = []
        nos = []
        for o in device.road_set:
            data_before.append({
                "no": o.no,
                "item": {
                    "id": o.item.id,
                    "name": o.item.name,
                } if o.item else {},
                "amount": o.amount,
            })
            nos.append(o.no)

        data_after = []
        for d in roads_info:
            if d["no"] not in nos:
                return {
                    "resultCode": 1,
                    "resultMsg": "货道信息错误"
                }
            if "item" not in d:
                continue
            item = Item.get_or_none(id=d["item"])
            data_after.append({
                "no": d["no"],
                "item": {
                    "id": item.id,
                    "name": item.name
                },
                "add": d["add"]
            })

        list_no = SupplyList.generate_no()
        obj = SupplyList.create(no=list_no,
                                device=device,
                                supplyer=device.supplyer,
                                data_before=json.dumps(data_before),
                                data_after=json.dumps(data_after))
        obj.save()

        helper = SMSHelper()
        helper.send_supply_message(device, obj)

        res = {
            "resultCode": 0,
            "resultMsg": "成功",
        }
        res.update(obj.to_dict())
        return res

    @rpc
    def finish_supply(self, device_id, no):
        "完成配货"
        obj = SupplyList.get_or_none(no=no)
        if not obj:
            return {
                "resultCode": 1,
                "resultMsg": "配货单号不存在",
            }

        if device_id != getattr(obj.device, "no", None):
            return {
                "resultCode": 1,
                "resultMsg": "该配货单与设备不符"
            }

        if obj.status == SupplyStatus.DONE:
            return {
                "resultCode": 1,
                "resultMsg": "配货单已经完成",
            }

        qs = SupplyList.update(status=SupplyStatus.DONE,
                               done_at=dte.now()).where(SupplyList.id == obj.id)
        qs.execute()

        biz = DeviceBiz(device=obj.device)
        data = json.loads(obj.data_after)
        for it in data:
            no = it["no"]
            item_id = it["item"]["id"]
            road = Road.get_or_none(device=obj.device, no=no)
            road.item = item_id
            road.save()
            biz.incr_road_stock(road, amount=it["add"])

        helper = SMSHelper()
        helper.send_finish_supply_message(obj)

        return{
            "resultCode": 0,
            "resultMsg": "成功",
        }

    @rpc
    def stat_overview_of_item(self, start_date, end_date, item=None):
        "关键指标"
        qs = DayItemStat.select().where(DayItemStat.day >= start_date,
                                        DayItemStat.day <= end_date)

        if item:
            qs = qs.where(DayItemStat.item == item)

        start_dte = dte.strptime(start_date, "%Y-%m-%d")
        end_dte = dte.strptime(end_date, "%Y-%m-%d")
        days = (end_dte - start_dte).days + 1

        total_qs = qs.select(
            fn.SUM(DayItemStat.clicks).alias("total_clicks"),
            fn.SUM(DayItemStat.visitors).alias("total_visitors"),
            fn.SUM(DayItemStat.orders_pay).alias("total_orders_pay"),
            fn.SUM(DayItemStat.users_pay).alias("total_users_pay"),
        )
        if total_qs.count() < 1:
            return {
                "resultCode": 0,
                "resultMsg": "OK",
                "totalClicks": 0,
                "avgClicks": 0,
                "totalVisitors": 0,
                "avgVisitors": 0,
                "totalOrdersPay": 0,
                "avgOrdersPay": 0,
                "conversion": 0,
            }
        total = total_qs.first()

        conversion = 0
        if total.total_visitors:
            conversion = float(total.total_users_pay) / int(total.total_visitors)

        return {
            "resultCode": 0,
            "resultMsg": "OK",
            "totalClicks": int(total.total_clicks),
            "avgClicks": int(total.total_clicks) / days,
            "totalVisitors": int(total.total_visitors),
            "avgVisitors": int(total.total_visitors) / days,
            "totalOrdersPay": int(total.total_orders_pay),
            "avgOrdersPay": int(total.total_orders_pay) / days,
            "conversion": "%.2f%%" % (conversion * 100)
        }

    @rpc
    def stat_overview_of_order(self, start_date, end_date, item=None):
        "关键指标"
        qs = DayItemStat.select().where(DayItemStat.day >= start_date,
                                        DayItemStat.day <= end_date)

        if item:
            qs = qs.where(DayItemStat.item == item)

        start_dte = dte.strptime(start_date, "%Y-%m-%d")
        end_dte = dte.strptime(end_date, "%Y-%m-%d")
        days = (end_dte - start_dte).days + 1

        total_qs = qs.select(
            fn.SUM(DayItemStat.sales_volume).alias("total_sales_volume"),
            fn.SUM(DayItemStat.orders_pay).alias("total_orders_pay"),
            fn.SUM(DayItemStat.users_pay).alias("total_users_pay"),
        )
        if total_qs.count() < 1:
            return {
                "resultCode": 0,
                "resultMsg": "OK",
                "totalSalesVolume": 0,
                "avgSalesVolume": 0,
                "totalOrdersPay": 0,
                "avgOrdersPay": 0,
                "totalUsersPay": 0,
                "avgUsersPay": 0,
                "userUnitPrice": 0,
            }
        total = total_qs.first()

        return {
            "resultCode": 0,
            "resultMsg": "OK",
            "totalSalesVolume": int(total.total_sales_volume),
            "avgSalesVolume": int(total.total_sales_volume) / days,
            "totalOrdersPay": int(total.total_orders_pay),
            "avgOrdersPay": int(total.total_orders_pay) / days,
            "totalUsersPay": int(total.total_users_pay),
            "avgUsersPay": int(total.total_users_pay) / days,
            "userUnitPrice": float(total.total_sales_volume) / int(total.total_orders_pay)
                            if int(total.total_orders_pay) else 0,
        }

    @rpc
    def stat_overview_of_device(self, start_date, end_date, device=None):
        "关键指标"
        qs = DayDeviceStat.select().where(DayDeviceStat.day >= start_date,
                                          DayDeviceStat.day <= end_date)

        if device:
            qs = qs.where(DayDeviceStat.device == device)

        start_dte = dte.strptime(start_date, "%Y-%m-%d")
        end_dte = dte.strptime(end_date, "%Y-%m-%d")
        days = (end_dte - start_dte).days + 1

        total_qs = qs.select(
            fn.SUM(DayDeviceStat.clicks).alias("total_clicks"),
            fn.SUM(DayDeviceStat.visitors).alias("total_visitors"),
            fn.SUM(DayDeviceStat.orders_pay).alias("total_orders_pay"),
            fn.SUM(DayDeviceStat.users_pay).alias("total_users_pay"),
        )

        if total_qs.count() < 1:
            return {
                "resultCode": 0,
                "resultMsg": "OK",
                "totalClicks": 0,
                "avgClicks": 0,
                "totalVisitors": 0,
                "avgVisitors": 0,
                "totalOrdersPay": 0,
                "avgOrdersPay": 0,
                "conversion": 0,
            }
        total = total_qs.first()

        conversion = 0
        if total.total_visitors:
            conversion = float(total.total_users_pay) / int(total.total_visitors)

        return {
            "resultCode": 0,
            "resultMsg": "OK",
            "totalClicks": int(total.total_clicks),
            "avgClicks": int(total.total_clicks) / days,
            "totalVisitors": int(total.total_visitors),
            "avgVisitors": int(total.total_visitors) / days,
            "totalOrdersPay": int(total.total_orders_pay),
            "avgOrdersPay": int(total.total_orders_pay) / days,
            "conversion": "%.2f%%" % (conversion * 100)
        }

    @rpc
    def stat_overview_of_user(self, start_date, end_date, user_group=None):
        "关键指标"

        start_dte = dte.strptime(start_date, "%Y-%m-%d")
        end_dte = dte.strptime(end_date, "%Y-%m-%d")
        days = (end_dte - start_dte).days + 1

        if user_group:
            qs = DayUserGroupStat.select(fn.SUM(DayUserGroupStat.users).alias("total_users"),
                                         fn.SUM(DayUserGroupStat.registers).alias("total_registers"),
                                         fn.SUM(DayUserGroupStat.actives).alias("total_actives")) \
                                 .where(DayUserGroupStat.day >= start_date,
                                        DayUserGroupStat.day <= end_date,
                                        DayUserGroupStat.user_group == user_group)
        else:
            qs = DayStat.select(fn.SUM(DayStat.users).alias("total_users"),
                                fn.SUM(DayStat.registers).alias("total_registers"),
                                fn.SUM(DayStat.actives).alias("total_actives")) \
                        .where(DayStat.day >= start_date,
                               DayStat.day <= end_date)

        if qs.count() < 1:
            return {
                "resultCode": 0,
                "resultMsg": "OK",
                "totalUsers": 0,
                "totalActives": 0,
                "avgActives": 0,
                "totalRegisters": 0,
                "avgRegisters": 0,
                "retention": 0,
            }

        total = qs .first()

        retention = 0
        if total.total_users:
            retention = float(total.total_actives) / int(total.total_users)

        return {
            "resultCode": 0,
            "resultMsg": "OK",
            "totalUsers": int(total.total_users),
            "totalActives": int(total.total_actives),
            "avgActives": int(total.total_actives) / days,
            "totalRegisters": int(total.total_registers),
            "avgRegisters": int(total.total_registers) / days,
            "retention": "%.2f%%" % (retention * 100),
        }

    @rpc
    def stat_day_of_item(self, start_date, end_date):
        total_qs = DayItemStat.select(DayItemStat.item,
                                      fn.SUM(DayItemStat.clicks).alias("total_clicks"),
                                      fn.SUM(DayItemStat.visitors).alias("total_visitors"),
                                      fn.SUM(DayItemStat.orders_pay).alias("total_orders_pay"),
                                      fn.SUM(DayItemStat.users_pay).alias("total_users_pay"),
                                      ) \
                              .group_by(DayItemStat.item) \
                              .where(DayItemStat.day >= start_date, DayItemStat.day <= end_date)

        items = []
        for o in total_qs:
            item = o.item
            conversion = 0
            if o.total_visitors:
                conversion = float(o.total_users_pay) / int(o.total_visitors)
            items.append({
                "itemNo": item.id,
                "itemName": item.name,
                "clicks": int(o.total_clicks),
                "visitors": int(o.total_visitors),
                "usersPay": int(o.total_users_pay),
                "ordersPay": int(o.total_orders_pay),
                "conversion": "%.2f%%" % (conversion * 100)
            })

        return {
            "resultCode": 0,
            "resultMsg": "OK",
            "items": items
        }

    @rpc
    def stat_day_of_user(self, start_date, end_date):
        qs = DayUserGroupStat.select(DayUserGroupStat.user_group,
                                     fn.SUM(DayUserGroupStat.users).alias("total_users"),
                                     fn.SUM(DayUserGroupStat.registers).alias("total_registers"),
                                     fn.SUM(DayUserGroupStat.sales_volume).alias("total_sales_volume"),
                                     fn.SUM(DayUserGroupStat.orders_pay).alias("total_orders_pay"),
                                     fn.SUM(DayUserGroupStat.actives).alias("total_actives")) \
                             .group_by(DayUserGroupStat.user_group) \
                             .where(DayUserGroupStat.day >= start_date,
                                    DayUserGroupStat.day <= end_date)

        items = []
        for o in qs:
            ug = o.user_group
            conversion = 0
            items.append({
                "userGroupId": ug.id,
                "userGroupName": ug.name,
                "users": int(o.total_users),
                "actives": int(o.total_actives),
                "registers": int(o.total_registers),
                "salesVolume": int(o.total_sales_volume),
                "ordersPay": int(o.total_orders_pay),
                "userUnitPrice": float(o.total_sales_volume) / int(o.total_orders_pay)
                            if int(o.total_orders_pay) else 0,
                "retention": "%.2f%%" % (conversion * 100)
            })

        return {
            "resultCode": 0,
            "resultMsg": "OK",
            "items": items
        }

    @rpc
    def stat_trend_of_user(self, start_date, end_date, user_group=None):
        """
        商品趋势：
        """
        if user_group:
            qs = DayUserGroupStat.select(DayUserGroupStat.day,
                                         fn.SUM(DayUserGroupStat.users).alias("total_users"),
                                         fn.SUM(DayUserGroupStat.registers).alias("total_registers"),
                                         fn.SUM(DayUserGroupStat.sales_volume).alias("total_sales_volume"),
                                         fn.SUM(DayUserGroupStat.orders_pay).alias("total_orders_pay"),
                                         fn.SUM(DayUserGroupStat.actives).alias("total_actives")) \
                                 .group_by(DayUserGroupStat.day) \
                                 .where(DayUserGroupStat.day >= start_date,
                                        DayUserGroupStat.day <= end_date,
                                        DayUserGroupStat.user_group == user_group)
        else:
            qs = DayStat.select(DayStat.day,
                                fn.SUM(DayStat.users).alias("total_users"),
                                fn.SUM(DayStat.registers).alias("total_registers"),
                                fn.SUM(DayStat.sales_volume).alias("total_sales_volume"),
                                fn.SUM(DayStat.orders_pay).alias("total_orders_pay"),
                                fn.SUM(DayStat.actives).alias("total_actives")) \
                        .group_by(DayStat.day) \
                        .where(DayStat.day >= start_date,
                               DayStat.day <= end_date)

        items = []
        for o in qs:
            items.append({
                "day": o.day,
                "users": int(o.total_users),
                "actives": int(o.total_actives),
                "registers": int(o.total_registers),
                "salesVolume": int(o.total_sales_volume),
                "ordersPay": int(o.total_orders_pay),
                "userUnitPrice": float(o.total_sales_volume) / int(o.total_orders_pay)
                            if int(o.total_orders_pay) else 0,
            })

        return {
            "resultCode": 0,
            "resultMsg": "OK",
            "days": items,
        }

    @rpc
    def stat_trend_of_device(self, start_date, end_date, device=None):
        """
        商品趋势：
        """
        total_qs = DayDeviceStat.select(DayDeviceStat.day,
                                        fn.SUM(DayDeviceStat.clicks).alias("total_clicks"),
                                        fn.SUM(DayDeviceStat.visitors).alias("total_visitors"),
                                        fn.SUM(DayDeviceStat.users_pay).alias("total_users_pay"),
                                        fn.SUM(DayDeviceStat.orders_pay).alias("total_orders_pay"),) \
                                .group_by(DayDeviceStat.day) \
                                .where(DayDeviceStat.day >= start_date, DayDeviceStat.day <= end_date)

        items = []
        for o in total_qs:
            conversion = 0
            if o.total_visitors:
                conversion = float(o.total_users_pay) / int(o.total_visitors)
            items.append({
                "day": o.day,
                "clicks": int(o.total_clicks),
                "visitors": int(o.total_visitors),
                "usersPay": int(o.total_users_pay),
                "ordersPay": int(o.total_orders_pay),
                "conversion": "%.2f%%" % (conversion * 100)
            })

        return {
            "resultCode": 0,
            "resultMsg": "OK",
            "days": items,
        }

    @rpc
    def stat_trend_of_item(self, start_date, end_date, item=None):
        """
        商品趋势：
        """
        total_qs = DayItemStat.select(DayItemStat.day,
                                      fn.SUM(DayItemStat.clicks).alias("total_clicks"),
                                      fn.SUM(DayItemStat.visitors).alias("total_visitors"),
                                      fn.SUM(DayItemStat.orders_pay).alias("total_orders_pay"),
                                      fn.SUM(DayItemStat.users_pay).alias("total_users_pay"),
                                      ) \
                              .group_by(DayItemStat.day) \
                              .where(DayItemStat.day >= start_date, DayItemStat.day <= end_date)

        items = []
        for o in total_qs:
            conversion = 0
            if o.total_visitors:
                conversion = float(o.total_users_pay) / int(o.total_visitors)
            items.append({
                "day": o.day,
                "clicks": int(o.total_clicks),
                "visitors": int(o.total_visitors),
                "usersPay": int(o.total_users_pay),
                "ordersPay": int(o.total_orders_pay),
                "conversion": "%.2f%%" % (conversion * 100)
            })

        return {
            "resultCode": 0,
            "resultMsg": "OK",
            "days": items,
        }

    @rpc
    def stat_trend_of_order(self, start_date, end_date):
        """
        商品趋势：
        """
        total_qs = DayItemStat.select(DayItemStat.day,
                                      fn.SUM(DayItemStat.clicks).alias("total_clicks"),
                                      fn.SUM(DayItemStat.visitors).alias("total_visitors"),
                                      fn.SUM(DayItemStat.orders_pay).alias("total_orders_pay"),
                                      fn.SUM(DayItemStat.users_pay).alias("total_users_pay"),
                                      fn.SUM(DayItemStat.sales_volume).alias("total_sales_volume"),
                                      ) \
                              .group_by(DayItemStat.day) \
                              .where(DayItemStat.day >= start_date, DayItemStat.day <= end_date)

        items = []
        for o in total_qs:
            conversion = 0
            if o.total_visitors:
                conversion = float(o.total_users_pay) / int(o.total_visitors)
            items.append({
                "day": o.day,
                "clicks": int(o.total_clicks),
                "visitors": int(o.total_visitors),
                "usersPay": int(o.total_users_pay),
                "ordersPay": int(o.total_orders_pay),
                "salesVolume": int(o.total_sales_volume),
                "conversion": "%.2f%%" % (conversion * 100),
                "userUnitPrice": float(o.total_sales_volume) / int(o.total_orders_pay)
                            if int(o.total_orders_pay) else 0,
            })

        return {
            "resultCode": 0,
            "resultMsg": "OK",
            "days": items,
        }

    @rpc
    def stat_conversion_of_order(self, start_date, end_date):
        """
        分布转化率
        """
        total_qs = DayDeviceStat.select(DayDeviceStat.day,
                                        fn.SUM(DayDeviceStat.orders_pay).alias("total_orders_pay"),
                                        fn.SUM(DayDeviceStat.item_clicks).alias("total_item_clicks"),
                                        fn.SUM(DayDeviceStat.flows).alias("total_flows"),
                                        fn.SUM(DayDeviceStat.stays).alias("total_stays")) \
                                .group_by(DayDeviceStat.day) \
                                .where(DayDeviceStat.day >= start_date, DayDeviceStat.day <= end_date)

        result = {}
        if total_qs.count() < 1:
            result = {
                "totalFlows": 0,
                "totalStays": 0,
                "totalOrdersCreate": 0,
                "totalOrdersPay": 0,
            }
        else:
            total = total_qs.first()
            result = {
                "totalFlows": int(total.total_flows),
                "totalStays": int(total.total_stays),
                "totalOrdersCreate": int(total.total_item_clicks),
                "totalOrdersPay": int(total.total_orders_pay),
            }

        result.update({
            "resultCode": 0,
            "resultMsg": "OK"
        })
        return result

    @rpc
    def get_export_order(self, start_date, end_date):
        """
        导出报表数据:会员-订单表
        """
        total_qs = Order.select()
        pass

    @rpc
    def get_export_inventory(self, start_date, end_date):
        """
        导出报表数据：实时库存
        """
        pass

    @rpc
    def get_export_user_monitor(self, start_date, end_date):
        """
        导出报表数据：人流监控
        """
        pass
