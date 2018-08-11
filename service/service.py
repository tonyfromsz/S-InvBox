# -*- coding: utf-8 -*-

import logging
import ujson as json
import selector as slt
import const as C

from datetime import datetime as dte, timedelta
from peewee import fn, JOIN
from util.rds import get_redis, RedisKeys
from models import (Device, Supplyer, Admin, UserGroup, ApkVersion, Advertisement,
                    Video, Image, ADImage, ADVideo, Item, Road, ItemBrand, Redeem,
                    RedeemActivity, ItemCategory, VoiceActivity, Order,
                    AddressType, DeviceCategory, DeviceGroup, SupplyList,
                    DayItemStat, DayDeviceStat, DayUserGroupStat, DayStat, User,
                    AddressAdmin, SponsorItem, SponsorAddress)
from util import md5, xml_to_dict
from base import BaseService, rpc, transaction_rpc
from selector import (UserSelectorProxy, SelectorProxy, ItemSelectorProxy,
                      ItemBrandSelectorProxy, ItemCategorySelectorProxy,
                      OrderSelectorProxy, DeviceSelectorProxy, RoadSelectorProxy,
                      RedeemSelectorProxy, VoiceWordSelectorProxy, AdminSelectorProxy,
                      DayDeviceStatProxy)
from const import (OrderStatus, PayStatus, PayType, SupplyStatus, RedeemStatus, RoadStatus)
from pay.manager import PayManager
from biz import OrderBiz, DeviceBiz, MarktingBiz
# from sms.helper import SMSHelper
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
        # TODO:返回管理范围： 1,补货员， 2,场地方——场地， 3,品牌方——品牌
        admin_range = []
        admin_range_2 = []
        if admin.role == 1:
            supply_obj = Supplyer.select().where(Supplyer.admin == admin.id)
            for obj in supply_obj:
                admin_range.append(obj.id)
        elif admin.role == 2:
            address_obj = AddressAdmin.select().where(AddressAdmin.admin == admin.id)
            for obj in address_obj:
                admin_range.append(obj.address_id)
        elif admin.role == 3:
            item_obj = SponsorItem.select().where(SponsorItem.admin == admin.id)
            for obj in item_obj:
                admin_range.append(obj.item_id)
            address_obj = SponsorAddress.select().where(SponsorAddress.admin == admin.id)
            for obj in address_obj:
                admin_range_2.append(obj.address_id)

        res = {
            "resultCode": 0,
            "resultMsg": "OK",
        }
        res.update(admin.to_dict())
        res.update({"range": admin_range})
        res.update({"infoList": admin_range_2})
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
    def get_orders(self, page=1, base_url="", page_size=10, query=[], export=False, admin_info=None):
        role = admin_info.get("role")
        admin_id = admin_info.get("id")
        # print(query)

        if role == 2:
            # print("role: 2")
            add_obj = AddressAdmin.select().where(AddressAdmin.admin == admin_id)
            if not add_obj.count():
                return {
                    "pageSize": page_size,
                    "totalCount": 0,
                    "page": page,
                    "items": {},
                }
            else:
                admin_range = []
                for obj in add_obj:
                    admin_range.append({
                        "operator": "=",
                        "attribute": "device__address_type",
                        "value": obj.address_id
                    })
                query.append(admin_range)
        elif role == 3:
            check_item = SponsorItem.select().where(SponsorItem.admin == admin_id)
            if not check_item.count():
                return {
                    "pageSize": page_size,
                    "totalCount": 0,
                    "page": page,
                    "items": {},
                }
            else:
                item_list = []
                for obj in check_item:
                    item_list.append({
                            "operator": "=",
                            "attribute": "item",
                            "value": obj.item_id
                        })
                query.append(item_list)

            check_address = SponsorAddress.select().where(SponsorAddress.admin == admin_id)
            if check_address.count():
                address_list = []
                for obj in check_address:
                    address_list.append({
                            "operator": "=",
                            "attribute": "device__address_type",
                            "value": obj.address_id
                        })
                query.append(address_list)

        def _parser(obj):
            device = obj.device
            road = obj.road
            item = obj.item
            user = obj.user
            redeem = obj.redeem
            d = {
                "id": obj.id,
                "no": obj.no,
                "device": {
                    "id": device.id,
                    "no": device.no,
                    "name": device.name,
                    "address": device.address_type.id
                },
                "redeem": {
                    "id": redeem.id,
                    "code": redeem.code
                },
                "road": {
                    "id": road.id,
                    "no": road.no,
                },
                "item": {
                    "id": item.id,
                    "name": item.name,
                    "no": item.no,
                    "brand": item.brand.id,
                    "brand_name": item.brand.name
                },
                "user": {
                    "id": user.id,
                    "mobile": user.mobile,
                    "wxuserid": user.wxuserid,
                    "username": user.username,
                    "aliuserid": user.aliuserid
                } if user else {},
                "count": obj.item_amount,
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
        if not export:
            return self.do_page(
                qs=OrderSelectorProxy(query).select(),
                page=page,
                item_parser=_parser
            )
        else:
            return self.do_export(
                qs=OrderSelectorProxy(query).select(),
                item_parser=_parser
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
    def get_roads(self, page=1, page_size=10, base_url="", query=[], admin_info=None, export=False):
        role = admin_info.get("role")
        admin_id = admin_info.get("id")
        admin_range = []
        if role == 1:
            sup_obj = Supplyer.select().where(Supplyer.admin == admin_id)
            if not sup_obj.count():
                return {
                    "pageSize": page_size,
                    "totalCount": 0,
                    "page": page,
                    "items": [],
                }
            else:
                for obj in sup_obj:
                    admin_range.append({
                        "operator": "=",
                        "attribute": "device__supplyer",
                        "value": obj.id
                    })
        elif role == 2:
            add_obj = AddressAdmin.select().where(AddressAdmin.admin == admin_id)
            if not add_obj.count():
                return {
                    "pageSize": page_size,
                    "totalCount": 0,
                    "page": page,
                    "items": [],
                }
            else:
                for obj in add_obj:
                    admin_range.append({
                        "operator": "=",
                        "attribute": "device__address_type",
                        "value": obj.address_id
                    })
        elif role == 3:
            admin_add = []
            item_obj = SponsorItem.select().where(SponsorItem.admin == admin_id)
            if not item_obj.count():
                return {
                    "pageSize": page_size,
                    "totalCount": 0,
                    "page": page,
                    "items": [],
                }
            else:
                for obj in item_obj:
                    admin_range.append({
                        "operator": "=",
                        "attribute": "item",
                        "value": obj.item_id
                    })

            add_obj = SponsorAddress.select().where(SponsorAddress.admin == admin_id)
            if add_obj.count():
                for obj in add_obj:
                    admin_add.append({
                        "operator": "=",
                        "attribute": "device__address_type",
                        "value": obj.address_id
                    })
                query.append(admin_add)

        query.append(admin_range)

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
                    "address_type": device.address_type.id
                },
                "item": {
                    "id": item.id,
                    "name": item.name,
                } if item else {},
                "amount": obj.amount,
                # "limit": road_meta_list[int(obj.no) - 1]["upper_limit"],
                "limit": 0,   # 测试用
                "status": obj.status_msg,
                "price": obj.price or getattr(item, "basic_price", 0),
                "thumbnails": [o.to_dict(base_url=base_url) for o in obj.thumbnails or getattr(item, "thumbnails", [])],
                "fault": obj.fault_msg,
                "faultAt": obj.fault_at.strftime("%Y-%m-%d %H:%M:%S")
                                if obj.fault_at else "",
                "updatedAt": obj.updated_at.strftime("%Y-%m-%d %H:%M:%S")
            }
            return d

        if not export:
            return self.do_page(
                qs=RoadSelectorProxy(query).select(),
                page=page,
                item_parser=_item_parser,
                page_size=page_size)
        else:
            return self.do_export(qs=RoadSelectorProxy(query).select(),
                                  item_parser=_item_parser)

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
    def get_test_data(self, args):
        return args

    @rpc
    def get_accounts(self, page=1, page_size=10):
        result = self.do_page(qs=Admin.select().where(Admin.role != 0),
                              page=page,
                              item_parser=Admin.to_dict,
                              page_size=page_size)

        items = result.get("items")
        supplyer = []
        add_type = []
        sponsor = []
        admin_dict = {}

        for item in items:
            role = item.get("role")
            admin_id = item.get('id')
            logger.info(role)
            admin_dict.update({admin_id: {"range": [], "infoList": []}})
            if role == 1:
                supplyer.append(item.get("id"))
            elif role == 2:
                add_type.append(item.get("id"))
            elif role == 3:
                sponsor.append(item.get("id"))

        supp_obj = Supplyer.select().where(Supplyer.admin.in_(supplyer))
        for obj in supp_obj:
            admin_dict.get(obj.admin_id)["range"].append(obj.id)

        add_obj = AddressAdmin.select().where(AddressAdmin.admin.in_(add_type))
        for obj in add_obj:
            admin_dict.get(obj.admin_id)["range"].append(obj.address_id)

        spon_item_obj = SponsorItem.select().where(SponsorItem.admin.in_(sponsor))
        for obj in spon_item_obj:
            admin_dict.get(obj.admin_id)["range"].append(obj.item_id)

        spon_add_obj = SponsorAddress.select().where(SponsorAddress.admin.in_(sponsor))
        for obj in spon_add_obj:
            admin_dict.get(obj.admin_id)["infoList"].append(obj.address_id)

        for item in items:
            item.update({"range": admin_dict.get(item["id"])})

        result.update({"items": items})
        return result

    @transaction_rpc
    def add_account(self, name, mobile, password, role, admin_range, info_list=None):
        if Admin.select().where(Admin.mobile == mobile).count():
            return {
                "resultCode": 1,
                "resultMsg": "手机号已存在%s" % mobile
            }
        if not admin_range:
            return {
                "resultCode": 1,
                "resultMsg": "请输入范围%s" % admin_range
            }

        # 补货员 admin_range=补货员id
        if role == 1:
            supplier_id = admin_range[0]
            check_supplier = Supplyer.get_or_none(Supplyer.id == supplier_id)
            if not check_supplier:
                return {
                    "resultCode": 1,
                    "resultMsg": "不存在该补货员%s" % admin_range
                }
            if check_supplier and check_supplier.mobile != mobile:
                return {
                    "resultCode": 1,
                    "resultMsg": "补货员手机号不匹配%s" % admin_range
                }
            else:
                pwd = md5(password)
                obj_admin = Admin.create(username=name, mobile=mobile, password=pwd, role=role)
                obj_admin.save()

                admin_id = Admin.get(mobile=mobile).id
                obj_supplier = Supplyer.update(admin=admin_id).where(Supplyer.id == supplier_id)
                obj_supplier.execute()

                res = {
                    "resultCode": 0,
                    "resultMsg": "成功",
                }
                res.update(obj_admin.to_dict())
                return res
        # 场地方 admin_range场地id的数组（AddressType.id）
        if role == 2:
            pwd = md5(password)
            obj_admin = Admin.create(username=name, mobile=mobile, password=pwd, role=role)
            obj_admin.save()

            admin_id = Admin.get(mobile=mobile).id

            # 如果不存在场地，抛出异常，事务回滚
            for addr in admin_range:
                addr_id = int(addr)
                check_addr = AddressType.get_or_none(AddressType.id == addr_id)
                if not check_addr:
                    raise KeyError
                check_addr_admin = AddressAdmin.get_or_none(address_id=addr_id, admin_id=admin_id)
                if check_addr_admin:
                    addr_admin_id = check_addr_admin.id
                    obj_addr_admin = AddressAdmin.update(update_at=dte.now()).where(id=addr_admin_id)
                    obj_addr_admin.execute()
                else:
                    obj_addr_admin = AddressAdmin.create(address_id=addr_id, admin_id=admin_id)
                    obj_addr_admin.save()

            res = {
                "resultCode": 0,
                "resultMsg": "成功",
            }
            res.update(obj_admin.to_dict())
            return res

        # 品牌方
        if role == 3:
            pwd = md5(password)
            obj_admin = Admin.create(username=name, mobile=mobile, password=pwd, role=role)
            obj_admin.save()

            admin_id = Admin.get(mobile=mobile).id
            # 品牌方-商品管理， 如出现没有商品的情况，抛出异常事务回滚，品牌方创建失败
            for item in admin_range:
                item_id = int(item)
                check_item = Item.get_or_none(Item.id == item_id)
                if not check_item:
                    raise KeyError
                check_sponsor_item = SponsorItem.get_or_none(admin_id=admin_id, item_id=item_id)
                if check_sponsor_item:
                    sponsor_item_id = check_sponsor_item.id
                    obj_sponsor_item = SponsorItem.update(update_at=dte.now()).where(id=sponsor_item_id)
                else:
                    obj_sponsor_item = SponsorItem.create(admin_id=admin_id, item_id=item_id)
                obj_sponsor_item.save()

            # 品牌方-场地管理，如出现没有场地传入的场地id的情况，抛出异常。事务回滚，品牌方创建失败
            for addr in info_list:
                addr_id = int(addr)
                check_addr = AddressType.get_or_none(AddressType.id == addr_id)
                if not check_addr:
                    raise KeyError
                check_sponsor_addr = SponsorAddress.get_or_none(admin_id=admin_id, address_id=addr_id)
                if check_sponsor_addr:
                    sponsor_addr_id = check_sponsor_addr.id
                    obj_sponsor_addr = SponsorAddress.update(update_at=dte.now()).where(id=sponsor_addr_id)
                    obj_sponsor_addr.execute()
                else:
                    obj_sponsor_addr = SponsorAddress.create(admin_id=admin_id, address_id=addr_id)
                    obj_sponsor_addr.save()

            res = {
                "resultCode": 0,
                "resultMsg": "成功",
            }
            res.update(obj_admin.to_dict())
            return res

    @transaction_rpc
    def delete_account(self, ids):
        delete_dict = {}
        admin_obj = Admin.select().where(Admin.id.in_(ids))
        for obj in admin_obj:
            delete_dict.update({obj.id: obj.role})

        for admin_id, role in delete_dict.items():
            if role == 1:
                q = Supplyer.update(admin=None).where(Supplyer.admin == admin_id)
            if role == 2:
                q = AddressAdmin.delete().where(AddressAdmin.admin == admin_id)
            if role == 3:
                q = SponsorItem.delete().where(SponsorItem.admin == admin_id)
                if SponsorAddress.get_or_none(SponsorAddress.admin == admin_id):
                    q2 = SponsorAddress.delete().where(SponsorAddress.admin == admin_id)
                    q2.execute()
            q.execute()

        q_admin = Admin.delete().where(Admin.id.in_(ids))
        q_admin.execute()
        return {
            "resultCode": 0,
            "resultMsg": "删除成功"
        }

    @transaction_rpc
    def modify_accounts(self, info_list):
        for info in info_list:
            admin_id = info.get('id')
            if not admin_id:
                return {
                    "resultCode": 1,
                    "resultMsg": "请传入id"
                }
            if not Admin.get_or_none(Admin.id == admin_id):
                return {
                    "resultCode": 1,
                    "resultMsg": "该用户不存在"
                }
            role = int(info.get("role"))

            update_dict = {}
            rang_list = info.get("admin_range")

            username = info.get("name")
            if username:
                update_dict.update({"username": username})

            password = info.get("password")
            if password:
                update_dict.update({"password": md5(password)})

            mobile = info.get("mobile")
            if mobile:
                if Admin.get_or_none(Admin.mobile == mobile, Admin.id != admin_id):
                    return {
                        "resultCode": 1,
                        "resultMsg": "该手机号被其他用户使用"
                    }
                else:
                    update_dict.update({"mobile": mobile})
            # 补货员更新
            if role == 1:
                supplier_list = info.get("admin_range")
                if supplier_list:
                    supplier_id = supplier_list[0]
                    if mobile:
                        if Supplyer.get_or_none(Supplyer.id == supplier_id, Supplyer.mobile != mobile):
                            return {
                                "resultCode": 1,
                                "resultMsg": "手机号无法匹配补货员"
                            }
                        else:
                            obj_supplier = Supplyer.update(admin=admin_id).where(Supplyer.id == supplier_id)
                            obj_supplier.execute()
                    else:
                        obj_supplier = Supplyer.update(admin=admin_id).where(Supplyer.id == supplier_id)
                        obj_supplier.execute()

            # 场地方更新
            elif role == 2:
                address_list = info.get("admin_range")
                if address_list:

                    # 删除减少的场地id记录
                    check_adress = AddressAdmin.select().where(AddressAdmin.admin == admin_id,
                                                               AddressAdmin.address.not_in(address_list))
                    delete_address_id = []
                    if check_adress.count():
                        for rec in check_adress:
                            delete_address_id.append(rec.id)
                        q = AddressAdmin.delete().where(AddressAdmin.id.in_(delete_address_id))
                        q.execute()

                    # 增加场地
                    for add_id in address_list:
                        check_exist_add = AddressAdmin.get_or_none(AddressAdmin.address == add_id,
                                                                   AddressAdmin.admin == admin_id)
                        if not check_exist_add:
                            obj_add_admin = AddressAdmin.create(admin=admin_id, address=add_id)
                            obj_add_admin.save()
                        else:
                            obj_add_admin = AddressAdmin.update(admin=admin_id, address=add_id, update_at=dte.now()).\
                                where(AddressAdmin.id == check_exist_add.id)
                            obj_add_admin.execute()

            # 更新品牌方
            elif role == 3:
                item_list = info.get("admin_range")
                if item_list:
                    check_item = SponsorItem.select().where(SponsorItem.admin == admin_id,
                                                            SponsorItem.item.not_in(item_list))
                    delete_item_id = []
                    if check_item.count():
                        for rec in check_item:
                            delete_item_id.append(rec.id)
                        q = SponsorItem.delete().where(SponsorItem.id.in_(delete_item_id))
                        q.execute()
                    for item_id in item_list:
                        check_exist_item = SponsorItem.get_or_none(SponsorItem.item == item_id,
                                                                   SponsorItem.admin == admin_id)
                        if not check_exist_item:
                            obj_item_admin = SponsorItem.create(admin=admin_id, item=item_id)
                            obj_item_admin.save()
                        else:
                            obj_item_admin = SponsorItem.update(admin=admin_id, item=item_id, update_at=dte.now()).\
                                where(SponsorItem.id == check_exist_item.id)
                            obj_item_admin.execute()

                # 修改品牌方管理场地
                address_list = info.get("info_list")
                if address_list:
                    # 删除减少的场地id记录
                    check_adress = SponsorAddress.select().where(SponsorAddress.admin == admin_id,
                                                                 SponsorAddress.address.not_in(address_list))
                    delete_address_id = []
                    if check_adress.count():
                        for rec in check_adress:
                            delete_address_id.append(rec.id)
                        q = SponsorAddress.delete().where(SponsorAddress.id.in_(delete_address_id))
                        q.execute()

                    # 增加场地
                    for add_id in address_list:
                        check_exist_add = SponsorAddress.get_or_none(SponsorAddress.address == add_id,
                                                                     SponsorAddress.admin == admin_id)
                        if not check_exist_add:
                            obj_add_admin = SponsorAddress.create(admin=admin_id, address=add_id)
                            obj_add_admin.save()
                        else:
                            obj_add_admin = SponsorAddress.update(admin=admin_id, address=add_id, update_at=dte.now())\
                                .where(SponsorAddress.id == check_exist_add.id)
                            obj_add_admin.execute()

            if not rang_list and not update_dict:
                return {
                    "resultCode": 1,
                    "resultMsg": "没有可更新的信息"
                }
            else:
                obj_admin = Admin.update(**update_dict).where(Admin.id == admin_id)
                obj_admin.execute()
                res = {
                    "resultCode": 0,
                    "resultMsg": "成功",
                }
                # res.update(obj_admin.to_dict())
                return res

    @rpc
    def get_flow_stats(self, page=1, base_url="", page_size=10, query=[], export=False, admin_info=None):
        role = admin_info.get("role")
        admin_id = admin_info.get("id")
        # print(query)

        if role == 3:
            check_item = SponsorItem.select().where(SponsorItem.admin == admin_id)
            if not check_item.count():
                return {
                    "pageSize": page_size,
                    "totalCount": 0,
                    "page": page,
                    "items": {},
                }
            else:
                item_list = []
                for obj in check_item:
                    item_list.append(obj.item_id)

                check_device = Road.select(fn.Distinct(Road.device)).where(Road.item.in_(item_list))
                if not check_device.count():
                    return {
                        "pageSize": page_size,
                        "totalCount": 0,
                        "page": page,
                        "items": {},
                }
                else:
                    device_list = []
                    for obj in check_device:
                        print(obj.device_id)
                        device_list.append({
                            "operator": "=",
                            "attribute": "device",
                            "value": obj.device_id
                        })
                        query.append(device_list)

            check_address = SponsorAddress.select().where(SponsorAddress.admin == admin_id)
            if check_address.count():
                address_list = []
                for obj in check_address:
                    address_list.append({
                        "operator": "=",
                        "attribute": "device__address_type",
                        "value": obj.address_id
                    })
                query.append(address_list)

        def _parser(obj):
            d = {
                "day": obj.day,
                "device": obj.device_id,
                "address_type": obj.device.address_type.name,
                "flows": obj.flows,
                "stays": obj.stays,
                "clicks": obj.clicks,
            }
            return d

        if not export:
            return self.do_page(
                qs=DayDeviceStatProxy(query).select(),
                page=page,
                item_parser=_parser
            )
        else:
            return self.do_export(
                qs=DayDeviceStatProxy(query).select(),
                item_parser=_parser
            )

    @rpc
    def dashboard_flow_volume(self):
        # 現在
        now = dte.now()
        # 當日
        from_day = dte(now.year, now.month, now.day, 0, 0, 0)
        # 當周
        week_first_day = now - timedelta(now.weekday())
        from_week = dte(week_first_day.year, week_first_day.month, week_first_day.day, 0, 0, 0)
        # 當年
        from_year = dte(now.year, 1, 1, 0, 0, 0)

        date_params = {
            "today": from_day,
            "week": from_week,
            "year": from_year
        }

        flow_volume_date = {
            "today": {
                "flows": 0,
                "stays": 0,
                "clicks": 0,
                "usersPay": 0,
                "staysConversion": 0,
                "clicksConversion": 0,
                "payConversion": 0,
                "startTime": "",
                "endTime": ""
            },
            "week": {
                "flows": 0,
                "stays": 0,
                "clicks": 0,
                "usersPay": 0,
                "staysConversion": 0,
                "clicksConversion": 0,
                "payConversion": 0,
                "startTime": "",
                "endTime": ""
            },
            "year": {
                "flows": 0,
                "stays": 0,
                "clicks": 0,
                "usersPay": 0,
                "staysConversion": 0,
                "clicksConversion": 0,
                "payConversion": 0,
                "startTime": "",
                "endTime": ""
            },
            "avg_device": {
                "flows": 0,
                "stays": 0,
                "clicks": 0,
                "usersPay": 0,
                "startTime": "",
                "endTime": ""
            }
        }
        # online_device = Device.select().where(Device.online is True).count()
        query = [[
            {
                "operator": "是",
                "attribute": "online",
                "value": True
            }
        ]]

        online_device = DeviceSelectorProxy(query).select().count()
        print("online_device:", online_device)

        for zoom, date in date_params.items():
            qs = DayDeviceStat.select().where(DayDeviceStat.created_at >= date,
                                              DayDeviceStat.created_at <= now)
            total_qs = qs.select(
                fn.sum(DayDeviceStat.flows).alias("total_flows"),
                fn.sum(DayDeviceStat.stays).alias("total_stays"),
                fn.sum(DayDeviceStat.clicks).alias("total_clicks"),
                fn.sum(DayDeviceStat.users_pay).alias("total_users_pay"),
            )
            flow_volume_date[zoom]["startTime"] = date.strftime("%Y-%m-%d %H:%M:%S")
            flow_volume_date[zoom]["endTime"] = now.strftime("%Y-%m-%d %H:%M:%S")
            if not total_qs.count():
                continue
            total = total_qs.first()

            stays_conversion = (float(total.total_stays) / int(total.total_flows)) if int(total.total_flows) else 0
            clicks_conversion = (float(total.total_clicks) / int(total.total_stays)) if int(total.total_stays) else 0
            pay_conversion = (float(total.total_users_pay) / int(total.total_clicks)) if int(total.total_clicks) else 0

            flow_volume_date[zoom]["flows"] = int(total.total_flows)
            flow_volume_date[zoom]["stays"] = int(total.total_stays)
            flow_volume_date[zoom]["clicks"] = int(total.total_clicks)
            flow_volume_date[zoom]["usersPay"] = int(total.total_users_pay)
            flow_volume_date[zoom]["staysConversion"] = "%.2f%%" % (stays_conversion * 100)
            flow_volume_date[zoom]["clicksConversion"] = "%.2f%%" % (clicks_conversion * 100)
            flow_volume_date[zoom]["payConversion"] = "%.2f%%" % (pay_conversion * 100)

            if zoom == "week" and online_device:
                flow_volume_date["avg_device"]["flows"] = int(total.total_flows / online_device)
                flow_volume_date["avg_device"]["stays"] = int(total.total_stays / online_device)
                flow_volume_date["avg_device"]["clicks"] = int(total.total_clicks / online_device)
                flow_volume_date["avg_device"]["usersPay"] = int(total.total_users_pay / online_device)
                flow_volume_date["avg_device"]["startTime"] = date.strftime("%Y-%m-%d %H:%M:%S")
                flow_volume_date["avg_device"]["endTime"] = now.strftime("%Y-%m-%d %H:%M:%S")

        return flow_volume_date

    @rpc
    def dashboard_flow_volume_rank(self):
        # 現在
        now = dte.now()
        # 當日
        from_day = dte(now.year, now.month, now.day, 0, 0, 0)
        # 當周
        week_first_day = now - timedelta(now.weekday())
        from_week = dte(week_first_day.year, week_first_day.month, week_first_day.day, 0, 0, 0)
        # 當年
        from_year = dte(now.year, 1, 1, 0, 0, 0)

        date_params = {
            "today": from_day,
            "week": from_week,
            "year": from_year
        }
        top_5_rank = {
            "today": {
                "flows": {
                    "device": [],
                    "count": []
                },
                "stays": {
                    "device": [],
                    "count": []
                },
                "clicks": {
                    "device": [],
                    "count": []
                }
            },
            "week": {
                "flows": {
                    "device": [],
                    "count": []
                },
                "stays": {
                    "device": [],
                    "count": []
                },
                "clicks": {
                    "device": [],
                    "count": []
                },
            },
            "year": {
                "flows": {
                    "device": [],
                    "count": []
                },
                "stays": {
                    "device": [],
                    "count": []
                },
                "clicks": {
                    "device": [],
                    "count": []
                },
            },
        }
        for zoom, date in date_params.items():
            # 經過
            flows_qs = DayDeviceStat.select(Device, DayDeviceStat)\
                .join(Device, JOIN.LEFT_OUTER)\
                .where(DayDeviceStat.created_at >= date,
                       DayDeviceStat.created_at <= now)\
                .order_by(DayDeviceStat.flows.desc())
            if flows_qs.count() >= 5:
                flows_rank = 5
            else:
                flows_rank = flows_qs.count()

            flows_list = []
            flows_device_list = []
            for obj in flows_qs:
                print(obj.device.name, obj.flows)
                flows_device_list.append(obj.device.name)
                flows_list.append(obj.flows)
            top_5_rank[zoom]["flows"]["device"] = flows_device_list[:flows_rank]
            top_5_rank[zoom]["flows"]["count"] = flows_list[:flows_rank]

            # 停留
            stays_qs = DayDeviceStat.select(Device, DayDeviceStat) \
                .join(Device, JOIN.LEFT_OUTER) \
                .where(DayDeviceStat.created_at >= date,
                       DayDeviceStat.created_at <= now) \
                .order_by(DayDeviceStat.stays.desc())
            if stays_qs.count() >= 5:
                stays_rank = 5
            else:
                stays_rank = stays_qs.count()

            rank_stays_list = []
            rank_devive_name_list = []

            for obj in stays_qs:
                print(obj.device.name, obj.stays)
                rank_devive_name_list.append(obj.device.name)
                rank_stays_list.append(obj.stays)

            # 點擊
            top_5_rank[zoom]["stays"]["device"] = rank_devive_name_list[:stays_rank]
            top_5_rank[zoom]["stays"]["count"] = rank_stays_list[:stays_rank]

            clicks_qs = DayDeviceStat.select(Device, DayDeviceStat) \
                .join(Device, JOIN.LEFT_OUTER) \
                .where(DayDeviceStat.created_at >= date,
                       DayDeviceStat.created_at <= now) \
                .order_by(DayDeviceStat.clicks.desc())
            if clicks_qs.count() >= 5:
                rank = 5
            else:
                rank = clicks_qs.count()
            rank_clicks_list = []
            rank_device_clicks_list = []
            for obj in clicks_qs:
                print(obj.device.name, obj.clicks)
                rank_device_clicks_list.append(obj.device.name)
                rank_clicks_list.append(obj.clicks)
            top_5_rank[zoom]["clicks"]["device"] = rank_device_clicks_list[:rank]
            top_5_rank[zoom]["clicks"]["count"] = rank_clicks_list[:rank]

        return top_5_rank

    @rpc
    def dashboard_user_stats(self):
        # 現在
        now = dte.now()
        # 當日
        from_day = dte(now.year, now.month, now.day, 0, 0, 0)
        # 當周
        week_first_day = now - timedelta(now.weekday())
        from_week = dte(week_first_day.year, week_first_day.month, week_first_day.day, 0, 0, 0)
        # 當年
        from_year = dte(now.year, 1, 1, 0, 0, 0)

        date_params = {
            "today": from_day,
            "week": from_week,
            "year": from_year
        }

        user_stats = {
            "today": {
                "newUsers": 0,
                "newFans": 0,
                "fansBuyRate": 0
            },
            "week": {
                "newUsers": 0,
                "newFans": 0,
                "fansBuyRate": 0
            },
            "year": {
                "newUsers": 0,
                "newFans": 0,
                "fansBuyRate": 0
            },
        }
        for zoom, date in date_params.items():
            new_user_qs = User.select().where(User.created_at >= date,
                                              User.created_at <= now,
                                              User.mobile == "",
                                              User.first_buy_at.is_null(False))
            new_fans_qs = User.select().where(User.created_at >= date,
                                              User.created_at <= now,
                                              User.mobile != "")

            fans_buy_qs = Order.select()\
                .join(User, JOIN.LEFT_OUTER)\
                .where(
                Order.created_at >= date,
                Order.created_at <= now,
                Order.pay_status != 1,
                User.mobile != "",
            )
            print(fans_buy_qs.count())
            total_buy_qs = Order.select() \
                .join(User, JOIN.LEFT_OUTER) \
                .where(Order.created_at >= date,
                       Order.created_at <= now,
                       Order.pay_status != 1)
            print(total_buy_qs.count())
            fans_buy_rate = (float(fans_buy_qs.count()) / int(total_buy_qs.count())) if int(total_buy_qs.count()) else 0

            user_stats[zoom]["newUsers"] = new_user_qs.count() or 0
            user_stats[zoom]["newFans"] = new_fans_qs.count() or 0
            user_stats[zoom]["fansBuyRate"] = "%.2f%%" % (fans_buy_rate * 100)

        return user_stats

    @rpc
    def dashboard_device_stats(self):
        # 現在
        now = dte.now()
        # 當日
        from_day = dte(now.year, now.month, now.day, 0, 0, 0)
        device_stats = {
            "involved_device": {
                "count": 0
            },
            "online_device": {
                "count": 0,
                'rate': 0
            },
            "active_device": {
                "count": 0,
                'rate': 0
            },
            "nonactive_device": {
                "count": 0,
                'rate': 0
            }
        }

        query = [[
            {
                "operator": "是",
                "attribute": "online",
                "value": True
            }
        ]]

        online_device = int(DeviceSelectorProxy(query).select().count())

        # device_online_qs = Device.select().where(Device.online is True)
        device_involved_qs = Device.select().where(Device.involved == 1)
        device_active_qs = Order.select(fn.Count(fn.Distinct(Order.device)).alias("active_device"))\
            .where(Order.created_at >= from_day,
                   Order.created_at <= now)

        involved_device_count = int(device_involved_qs.count()) if device_involved_qs.count() else 0
        online_device_count = online_device
        active_device_count = int(device_active_qs.first().active_device) if device_active_qs.first().active_device else 0

        online_device_rate = (float(online_device_count) / involved_device_count) \
            if involved_device_count else 0
        active_device_rate = (float(active_device_count) / online_device_count) \
            if online_device_count else 0
        nonactive_device_rate = float(online_device_count-active_device_count) / online_device_count \
            if online_device_count else 0

        device_stats["involved_device"]["count"] = involved_device_count
        device_stats["online_device"]["count"] = online_device_count
        device_stats["online_device"]["rate"] = "%.2f%%" % (online_device_rate * 100)
        device_stats["active_device"]["count"] = active_device_count
        device_stats["active_device"]["rate"] = "%.2f%%" % (active_device_rate * 100)
        device_stats["nonactive_device"]["count"] = online_device_count - active_device_count
        device_stats["nonactive_device"]["rate"] = "%.2f%%" % (nonactive_device_rate * 100)

        return device_stats

    @rpc
    def dashboard_sales_stats(self):
        # 現在
        now = dte.now()
        # 當日
        from_day = dte(now.year, now.month, now.day, 0, 0, 0)
        # 當周
        week_first_day = now - timedelta(now.weekday())
        from_week = dte(week_first_day.year, week_first_day.month, week_first_day.day, 0, 0, 0)
        # 當年
        from_year = dte(now.year, 1, 1, 0, 0, 0)

        date_params = {
            "today": from_day,
            "week": from_week,
            "year": from_year
        }
        sales_stats = {
            "today": {
                "sales_amount": 0,
                "item_amount": 0,
                "avg_amount": 0
            },
            "week": {
                "sales_amount": 0,
                "item_amount": 0,
                "avg_amount": 0,
                "sale_per_device": 0,
                "item_per_device": 0
            },
            "year": {
                "sales_amount": 0,
                "item_amount": 0,
                "avg_amount": 0
            },
        }
        query = [[
            {
                "operator": "是",
                "attribute": "online",
                "value": True
            }
        ]]

        online_device = int(DeviceSelectorProxy(query).select().count())

        for zoom, date in date_params.items():
            sale_qs = Order.select(fn.SUM(Order.pay_money).alias("sales_amount"),
                                   fn.SUM(Order.item_amount).alias("item_amount"))\
                .where(Order.pay_status != 1,
                       Order.created_at >= date,
                       Order.created_at <= now)
            if sale_qs.count() < 1:
                return sales_stats
            else:
                sales_amount = int(sale_qs.first().sales_amount)
                item_amount = int(sale_qs.first().item_amount)
                avg_amount = int(sales_amount / item_amount) if item_amount else 0
                sales_stats[zoom]["sales_amount"] = sales_amount
                sales_stats[zoom]["item_amount"] = item_amount
                sales_stats[zoom]["avg_amount"] = avg_amount
            if zoom == "week":
                sale_per_device = (float(sales_amount) / online_device) if online_device else 0
                item_per_device = (float(item_amount) / online_device) if online_device else 0
                sales_stats[zoom]["sale_per_device"] = "%.2f" % sale_per_device
                sales_stats[zoom]["item_per_device"] = "%.2f" % item_per_device

        return sales_stats

    @rpc
    def dashboard_item_device_rank(self):
        # 現在
        now = dte.now()
        # 當日
        from_day = dte(now.year, now.month, now.day, 0, 0, 0)
        # 當周
        week_first_day = now - timedelta(now.weekday())
        from_week = dte(week_first_day.year, week_first_day.month, week_first_day.day, 0, 0, 0)
        # 當年
        from_year = dte(now.year, 1, 1, 0, 0, 0)

        date_params = {
            "today": from_day,
            "week": from_week,
            "year": from_year
        }
        item_device_rank = {
            "today": {
                "itemSales": [],
                "topSalesItems": [],
                "itemAmount": [],
                "topAmountItems": [],
                "deviceSales": [],
                "topSalesDevice": [],
                "deviceAmount": [],
                "topAmountDevices": [],
                "userBuys": [],
                "topUsers": []
            },
            "week": {
                "itemSales": [],
                "topSalesItems": [],
                "itemAmount": [],
                "topAmountItems": [],
                "deviceSales": [],
                "topSalesDevice": [],
                "deviceAmount": [],
                "topAmountDevices": [],
                "userBuys": [],
                "topUsers": []
            },
            "year": {
                "itemSales": [],
                "topSalesItems": [],
                "itemAmount": [],
                "topAmountItems": [],
                "deviceSales": [],
                "topSalesDevice": [],
                "deviceAmount": [],
                "topAmountDevices": [],
                "userBuys": [],
                "topUsers": []
            }
        }
        for zoom, date in date_params.items():
            # 產品銷售金額排行
            sale_qs = Order.select(Item, fn.SUM(Order.pay_money).alias("item_sale"))\
                .join(Item)\
                .where(Order.created_at >= date,
                       Order.created_at <= now,
                       Order.redeem.is_null(True)
                       )\
                .group_by(Order.item)\
                .order_by(fn.SUM(Order.pay_money).desc())

            if sale_qs.count() >= 5:
                sale_rank = 5
            else:
                sale_rank = sale_qs.count()

            # print(sale_qs.count())

            sale_list = []
            top_sale_item_list = []
            for obj in sale_qs:
                sale_list.append(float(obj.item_sale))
                top_sale_item_list.append(str(obj.item.name))

            item_device_rank[zoom]["itemSales"] = sale_list[:sale_rank]
            item_device_rank[zoom]["topSalesItems"] = top_sale_item_list[:sale_rank]

            # 產品銷售量排行
            amount_qs = Order.select(Item, fn.SUM(Order.item_amount).alias("item_amount"))\
                .join(Item)\
                .where(Order.created_at >= date,
                       Order.created_at <= now,
                       Order.redeem.is_null(True))\
                .group_by(Order.item)\
                .order_by(fn.SUM(Order.item_amount).desc())
            if amount_qs.count() >= 5:
                amount_rank = 5
            else:
                amount_rank = sale_qs.count()

            # print(amount_qs.count())
            amount_list = []
            top_amount_item_list = []
            for obj in amount_qs:
                amount_list.append(float(obj.item_amount))
                top_amount_item_list.append(str(obj.item.name))

            item_device_rank[zoom]["itemAmount"] = amount_list[:amount_rank]
            item_device_rank[zoom]["topAmountItems"] = top_amount_item_list[:amount_rank]

            # 單機銷售金額排行
            sale_qs = Order.select(Device, fn.SUM(Order.pay_money).alias("device_sale")) \
                .join(Device) \
                .where(Order.created_at >= date,
                       Order.created_at <= now,
                       Order.redeem.is_null(True)) \
                .group_by(Order.device) \
                .order_by(fn.SUM(Order.pay_money).desc())

            if sale_qs.count() >= 5:
                sale_rank = 5
            else:
                sale_rank = sale_qs.count()

            # print(sale_qs.count())
            sale_list = []
            top_sale_device_list = []
            for obj in sale_qs:
                sale_list.append(float(obj.device_sale))
                top_sale_device_list.append(str(obj.device.name))

            item_device_rank[zoom]["deviceSales"] = sale_list[:sale_rank]
            item_device_rank[zoom]["topSalesDevice"] = top_sale_device_list[:sale_rank]

            # 單機銷售量排行
            amount_qs = Order.select(Device, fn.SUM(Order.pay_money).alias("device_amount")) \
                .join(Device) \
                .where(Order.created_at >= date,
                       Order.created_at <= now,
                       Order.redeem.is_null(True)) \
                .group_by(Order.device) \
                .order_by(fn.SUM(Order.pay_money).desc())

            if amount_qs.count() >= 5:
                amount_rank = 5
            else:
                amount_rank = sale_qs.count()

            # print(amount_qs.count())
            amount_list = []
            top_amount_device_list = []
            for obj in amount_qs:
                amount_list.append(float(obj.device_amount))
                top_amount_device_list.append(str(obj.device.name))

            item_device_rank[zoom]["deviceAmount"] = amount_list[:amount_rank]
            item_device_rank[zoom]["topAmountDevices"] = top_amount_device_list[:amount_rank]

            # 用戶
            user_qs = Order.select(User, fn.COUNT(Order.id).alias("users_buys")) \
                .join(User) \
                .where(Order.created_at >= date,
                       Order.created_at <= now,
                       Order.redeem.is_null(True)) \
                .group_by(Order.user) \
                .order_by(fn.COUNT(Order.id).desc())

            if amount_qs.count() >= 5:
                user_rank = 5
            else:
                user_rank = sale_qs.count()

            # print(amount_qs.count())

            buy_list = []
            top_user_list = []
            for obj in user_qs:
                buy_list.append(float(obj.users_buys))
                top_user_list.append(obj.user.id)

            item_device_rank[zoom]["userBuys"] = buy_list[:user_rank]
            item_device_rank[zoom]["topUsers"] = top_user_list[:user_rank]

        return item_device_rank

    @rpc
    def dashboard_pay_conversion_trend(self):
        now = dte.now()
        # 當周
        week_first_day = now - timedelta(now.weekday())
        from_week = dte(week_first_day.year, week_first_day.month, week_first_day.day, 0, 0, 0)
        from_last_week = from_week - timedelta(days=7)

        pay_conversion_trend = {
            "lastWeekTrend": [],
            "thisWeekTrend": [],
            "lastWeekDate": [],
            "thisWeekDate": []
        }

        for day in range(7):
            # print(day)
            from_time = from_last_week + timedelta(days=day)
            to_time = from_last_week + timedelta(days=day+1)
            pay_conversion_trend["lastWeekDate"].append(from_time.strftime("%m-%d"))
            qs = DayDeviceStat.select().where(DayDeviceStat.created_at >= from_time,
                                              DayDeviceStat.created_at <= to_time)
            total_qs = qs.select(
                fn.sum(DayDeviceStat.clicks).alias("total_clicks"),
                fn.sum(DayDeviceStat.users_pay).alias("total_users_pay"),
            )
            if not total_qs.count():
                pay_conversion_trend["lastWeekTrend"].append(None)
                continue
            total = total_qs.first()
            if not total.total_users_pay or not total.total_clicks:
                pay_conversion_trend["lastWeekTrend"].append("%.2f%%" % 0)
            else:
                pay_conversion = (float(total.total_users_pay) / int(total.total_clicks)) if int(total.total_clicks) else 0
                pay_conversion_trend["lastWeekTrend"].append("%.2f%%" % (pay_conversion * 100))

            # 本周
            from_this_time = from_week + timedelta(days=day)
            to_this_time = from_week + timedelta(days=day + 1)
            # print(from_time, to_time)
            pay_conversion_trend["thisWeekDate"].append(from_this_time.strftime("%m-%d"))

            qs = DayDeviceStat.select().where(DayDeviceStat.created_at >= from_this_time,
                                              DayDeviceStat.created_at <= to_this_time)
            total_qs = qs.select(
                fn.sum(DayDeviceStat.clicks).alias("total_clicks"),
                fn.sum(DayDeviceStat.users_pay).alias("total_users_pay"),
            )
            if not total_qs.count():
                pay_conversion_trend["thisWeekTrend"].append("%.2f%%" % 0)
                continue
            total = total_qs.first()
            if not total.total_users_pay or not total.total_clicks:
                pay_conversion_trend["thisWeekTrend"].append("%.2f%%" % 0)
            else:
                pay_conversion = (float(total.total_users_pay) / int(total.total_clicks)) if int(total.total_clicks) else 0
                pay_conversion_trend["thisWeekTrend"].append("%.2f%%" % (pay_conversion * 100))
                # print("total_users_pay", total.total_users_pay)
                # print("total_clicks", int(total.total_clicks))

        return pay_conversion_trend