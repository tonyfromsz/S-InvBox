# coding:utf-8

import random
import logging
import peewee as pw
import ujson as json
import const as C

from datetime import datetime as dte
from config import config
from const import (RedeemStatus, RoadStatus, FaultType, RoadStatusMsg, FaultMsg,
                   SupplyStatus)
from playhouse.fields import ManyToManyField

logger = logging.getLogger(__name__)


if config.database == "sqlite":
    db = pw.SqliteDatabase(':memory:')
else:
    db = pw.MySQLDatabase(config.mysql["database"], **{
        'host': config.mysql["host"],
        'port': config.mysql["port"],
        'user': config.mysql["user"],
        "password": config.mysql["password"],
    })


class BaseModel(pw.Model):
    class Meta:
        database = db

    @classmethod
    def get_or_none(cls, *args, **kwargs):
        query = []
        if kwargs:
            query = [getattr(cls, k) == v for k, v in kwargs.items()]
        if args:
            query.extend(list(args))

        try:
            obj = cls.get(*query)
            return obj
        except cls.DoesNotExist:
            return None

    @classmethod
    def get_or_create(cls, **kwargs):
        obj = cls.get_or_none(**kwargs)
        if not obj:
            if cls == Device:
                from biz import DeviceBiz
                biz = DeviceBiz()
                obj = biz.create(**kwargs)
            else:
                obj = cls.create(**kwargs)
                obj.save()
        return obj

    @property
    def key(self):
        return self.id

    def reload(self):
        if self.is_dirty():
            logger.warning("fields is dirty! <%s:%s>", self._meta.name, self._dirty)

        newer_self = self.get(self._meta.primary_key == self._get_pk_value())
        for field_name in self._meta.fields.keys():
            val = getattr(newer_self, field_name)
            setattr(self, field_name, val)
        self._dirty.clear()


class Image(BaseModel):
    id = pw.PrimaryKeyField()
    md5 = pw.CharField(unique=True)
    url = pw.CharField()
    created_at = pw.DateTimeField(default=dte.now)

    def to_dict(self, base_url=""):
        return {
            "id": self.id,
            "md5": self.md5,
            "url": base_url + self.url,
        }


class Video(BaseModel):

    id = pw.PrimaryKeyField()
    md5 = pw.CharField(unique=True, index=True)
    url = pw.CharField()
    created_at = pw.DateTimeField(default=dte.now)

    def to_dict(self, base_url=""):
        url = self.url
        if not self.url.startswith("http"):
            url = base_url + self.url

        return {
            "id": self.id,
            "md5": self.md5,
            "url": url
        }


class Admin(BaseModel):
    """
    系统用户
    """
    id = pw.PrimaryKeyField()
    mobile = pw.CharField(unique=True, index=True, max_length=11)        # 手机号码
    username = pw.CharField(unique=True, index=True)        # 用户名
    password = pw.CharField(default="default")              # 密码
    role = pw.IntegerField(default=0, index=True)           # 角色
    created_at = pw.DateTimeField(default=dte.now)          # 注册时间

    class Meta:
        db_table = 'admin'

    def to_dict(self):
        return {
            "id": self.id,
            "username": self.username,
            "mobile": self.mobile,
            "role": self.role,
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S")
        }


class Supplyer(BaseModel):
    """
    补货员
    """
    id = pw.PrimaryKeyField()
    mobile = pw.CharField(unique=True, index=True, max_length=11)           # 手机号码
    name = pw.CharField(default="")                                         # 名字
    created_at = pw.DateTimeField(default=dte.now)          # 注册时间

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "mobile": self.mobile,
            "deviceCount": self.device_set.count(),
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S")
        }


class ADImage(BaseModel):
    """
    广告图片
    """
    id = pw.PrimaryKeyField()
    name = pw.CharField(default="")
    image = pw.ForeignKeyField(Image)
    created_at = pw.DateTimeField(default=dte.now)          # 创建时间

    def to_dict(self, base_url=""):
        return {
            "id": self.id,
            "name": self.name,
            "image": self.image.to_dict(base_url=base_url)
                            if self.image else "",
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S")
        }

    @property
    def image_url(self):
        return self.image.url if self.image else ""

    @property
    def md5(self):
        return self.image.md5 if self.image else ""


class ADVideo(BaseModel):
    """
    广告视频
    """
    id = pw.PrimaryKeyField()
    name = pw.CharField(default="")
    video = pw.ForeignKeyField(Video)
    created_at = pw.DateTimeField(default=dte.now)          # 创建时间

    def to_dict(self, base_url=""):
        return {
            "id": self.id,
            "name": self.name,
            "video": self.video.to_dict(base_url=base_url)
                            if self.video else "",
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S")
        }

    @property
    def video_url(self):
        return self.video.url if self.video else ""

    @property
    def md5(self):
        return self.video.md5 if self.video else ""


class ApkVersion(BaseModel):
    """
    apk版本
    """
    id = pw.PrimaryKeyField()
    version = pw.CharField(unique=True, index=True)         # 版本号
    up_type = pw.IntegerField(null=True)                    # 升级类型
    url = pw.CharField(null=True)                           # 下载地址
    created_at = pw.DateTimeField(default=dte.now)          # 创建时间

    class Meta:
        db_table = 'apk'


class DeviceCategory(BaseModel):
    """
    设备型号

    road_meta_list:
    [
        {
            "no": "01",
            "upper_limit": 100,
            "lower_limit": 10,
        }
    ]
    """
    id = pw.PrimaryKeyField()
    name = pw.CharField(unique=True, index=True)
    road_count = pw.IntegerField(default=0)
    road_meta_list = pw.CharField(max_length=2048, default="[]")
    created_at = pw.DateTimeField(default=dte.now)          # 创建时间

    def to_dict(self):
        road_list = json.loads(self.road_meta_list)
        return {
            "id": self.id,
            "name": self.name,
            "roadCount": self.road_count,
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "roadList": [{
                "upperLimit": d["upper_limit"],
                "lowerLimit": d["lower_limit"]
            } for d in road_list],
        }


class AddressType(BaseModel):
    id = pw.PrimaryKeyField()
    name = pw.CharField(unique=True, index=True)
    created_at = pw.DateTimeField(default=dte.now)          # 创建时间

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S")
        }


class Device(BaseModel):
    """
    设备
    """
    ONLINE_SECONDS = 30

    id = pw.PrimaryKeyField()
    no = pw.CharField(unique=True, index=True)                  # 设备编号
    category = pw.ForeignKeyField(DeviceCategory)               # 设备型号
    name = pw.CharField(unique=True)                            # 名称
    involved = pw.BooleanField(default=False, index=True)       # 是否接入
    province = pw.CharField(default="")                         # 省
    city = pw.CharField(default="")                             # 市
    district = pw.CharField(default="")                         # 区
    address = pw.CharField(null=True)                           # 地址
    address_type = pw.ForeignKeyField(AddressType, null=True)   # 地址类型
    supplyer = pw.ForeignKeyField(Supplyer, null=True)      # 维护员；补货员
    mobile = pw.CharField(null=True, max_length=11)         # 设备手机号
    heartbeat_at = pw.DateTimeField(default=dte.now)
    door_opened = pw.BooleanField(default=False)            # 门是否打开
    is_stockout = pw.BooleanField(default=True, index=True)          # 是否缺货
    stockout_at = pw.DateTimeField(default=dte.now)
    created_at = pw.DateTimeField(default=dte.now)          # 创建时间
    updated_at = pw.DateTimeField(default=dte.now)

    class Meta:
        db_table = 'device'

    @property
    def online(self):
        seconds = (dte.now() - self.heartbeat_at).total_seconds()
        if seconds > self.ONLINE_SECONDS:
            return False
        return True


class DeviceGroup(BaseModel):
    "设备群"
    id = pw.PrimaryKeyField()
    name = pw.CharField()
    condition = pw.CharField(max_length=512, default="[]")
    created_at = pw.DateTimeField(default=dte.now)  # 创建时间

    class Meta:
        db_table = 'device_group'

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "condition": json.loads(self.condition),
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        }


class Advertisement(BaseModel):
    """
    绑定设备的广告

    命名规则：a_video1，a表示广告位编号，1表示视频编号。
    """

    id = pw.PrimaryKeyField()
    device = pw.ForeignKeyField(Device, unique=True)
    a_video1 = pw.ForeignKeyField(ADVideo, related_name="a1_deviceadvertisement_set", null=True)
    a_video2 = pw.ForeignKeyField(ADVideo, related_name="a2_deviceadvertisement_set", null=True)
    a_video3 = pw.ForeignKeyField(ADVideo, related_name="a3_deviceadvertisement_set", null=True)
    a_video4 = pw.ForeignKeyField(ADVideo, related_name="a4_deviceadvertisement_set", null=True)
    a_text = pw.CharField()
    b_image1 = pw.ForeignKeyField(ADImage, related_name="b_deviceadvertisement_set", null=True)
    c_image1 = pw.ForeignKeyField(ADImage, related_name="c_deviceadvertisement_set", null=True)

    class Meta:
        db_table = 'advertisement'

    def to_dict(self, base_url=""):
        return {
            "device": self.device.id,
            "aVideo1": self.a_video1.to_dict(base_url=base_url) if self.a_video1 else {},
            "aVideo2": self.a_video2.to_dict(base_url=base_url) if self.a_video2 else {},
            "aVideo3": self.a_video3.to_dict(base_url=base_url) if self.a_video3 else {},
            "aVideo4": self.a_video4.to_dict(base_url=base_url) if self.a_video4 else {},
            "aText": self.a_text,
            "bImage1": self.b_image1.to_dict(base_url=base_url) if self.b_image1 else {},
            "cImage1": self.c_image1.to_dict(base_url=base_url) if self.c_image1 else {},
        }


class ItemCategory(BaseModel):
    """
    商品类型
    """
    id = pw.PrimaryKeyField()
    name = pw.CharField(unique=True)

    thumbnail = pw.ForeignKeyField(Image, null=True,
                                   related_name="itemcategory_set1")
    image = pw.ForeignKeyField(Image, null=True,
                               related_name="itemcategory_set2")
    created_at = pw.DateTimeField(default=dte.now)

    class Meta:
        db_table = 'item_category'

    def to_dict(self, base_url=""):
        return {
            "id": self.id,
            "name": self.name,
            "thumbnail": self.thumbnail.to_dict(base_url=base_url)
                            if self.thumbnail else "",
            "image": self.image.to_dict(base_url=base_url)
                            if self.image else "",
            "itemCount": self.item_set.count(),
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S")
        }

    @property
    def thumbnail_url(self):
        return self.thumbnail.url if self.thumbnail else ""

    @property
    def image_url(self):
        return self.image.url if self.image else ""


class ItemBrand(BaseModel):
    """
    商品品牌
    """
    id = pw.PrimaryKeyField()
    name = pw.CharField(unique=True)
    created_at = pw.DateTimeField(default=dte.now)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "itemCount": self.item_set.count(),
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S")
        }


class Item(BaseModel):
    """
    商品
    """
    id = pw.PrimaryKeyField()
    no = pw.CharField(default="")                       # 商品编码；商品编号
    category = pw.ForeignKeyField(ItemCategory)         # 商品类型
    brand = pw.ForeignKeyField(ItemBrand)               # 商品品牌
    name = pw.CharField(null=True, unique=True, index=True)          # 商品名称
    thumbnails = ManyToManyField(Image, related_name="items")
    basic_price = pw.IntegerField(default=0)            # 价格；建议价; 并不是最终出售价格
    cost_price = pw.IntegerField(default=0)             # 成本价
    description = pw.CharField(null=True)               # 商品描述
    updated_at = pw.DateTimeField(default=dte.now)
    created_at = pw.DateTimeField(default=dte.now)

    class Meta:
        db_table = 'item'

    def to_dict(self, base_url=""):
        pv = self.order_set.count()
        sales = self.order_set.where(Order.status == C.OrderStatus.DONE).count()
        return {
            "id": self.id,
            "no": self.no,
            "sales": sales,         # 销量
            "pv": pv,               # 访问量
            "name": self.name,
            "category": self.category.to_dict(base_url=base_url),
            "brand": self.brand.to_dict() if self.brand else {},
            "basicPrice": self.basic_price,
            "costPrice": self.cost_price,
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "updatedAt": self.updated_at.strftime("%Y-%m-%d %H:%M:%S"),
            "thumbnails": [obj.to_dict(base_url=base_url)
                           for obj in self.thumbnails]
        }


class Road(BaseModel):
    """
    货道
    """
    id = pw.PrimaryKeyField()
    no = pw.CharField()                                 # 编号
    device = pw.ForeignKeyField(Device)                 # 对应的设备
    amount = pw.IntegerField(default=0)                 # 商品剩余数量
    status = pw.IntegerField(default=RoadStatus.SELLING)                 # 货道状态：取值定义请看const
    item = pw.ForeignKeyField(Item, null=True)         # 绑定的商品
    thumbnails = ManyToManyField(Image, related_name="roads")
    price = pw.IntegerField(default=0)                                # 出售价格
    fault = pw.IntegerField(default=FaultType.NONE, index=True)
    fault_at = pw.DateTimeField(null=True)
    updated_at = pw.DateTimeField(default=dte.now)
    created_at = pw.DateTimeField(default=dte.now)      # 创建时间

    class Meta:
        db_table = 'road'

        indexes = (
            (('device', 'no'), True),
        )

    @property
    def status_msg(self):
        return RoadStatusMsg[self.status]

    @property
    def fault_msg(self):
        return FaultMsg[self.fault]

    @property
    def sale_price(self):
        return self.price or getattr(self.item, "basic_price", 0)

    @property
    def sale_image_url(self):
        images = getattr(self, "thumbnails", []) or getattr(self.item, "thumbnails", [])
        return images[0].url if images else ""


class User(BaseModel):
    """
    用户
    """
    id = pw.PrimaryKeyField()
    icon = pw.CharField(null=True)                      # 用户头像
    mobile = pw.CharField(index=True, max_length=11, default="")    # 手机号
    username = pw.CharField(unique=True, index=True)    # 名字
    wxuserid = pw.CharField(default="", index=True)                 # 微信id
    aliuserid = pw.CharField(default="", index=True)                # 支付宝id
    birthday = pw.DateTimeField(null=True)
    first_buy_at = pw.DateTimeField(null=True)
    last_buy_at = pw.DateTimeField(null=True)
    created_at = pw.DateTimeField(default=dte.now)      # 创建时间

    class Meta:
        db_table = 'user'


class UserGroup(BaseModel):
    "会员群"
    id = pw.PrimaryKeyField()
    name = pw.CharField()
    condition = pw.CharField(max_length=512, default="[]")
    created_at = pw.DateTimeField(default=dte.now)  # 创建时间

    class Meta:
        db_table = 'user_group'

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "condition": json.loads(self.condition),
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        }


class RedeemActivity(BaseModel):
    "兑换活动"
    id = pw.PrimaryKeyField()
    name = pw.CharField(unique=True)
    valid_start_at = pw.DateTimeField()                 # 有效起始时间
    valid_end_at = pw.DateTimeField()                   # 有效截止时间
    item = pw.ForeignKeyField(Item)
    user_group = pw.ForeignKeyField(UserGroup)
    created_at = pw.DateTimeField(default=dte.now)

    class Meta:
        db_table = 'redeem_activity'

    def to_dict(self):
        ug = self.user_group
        item = self.item
        return {
            "id": self.id,
            "name": self.name,
            "validStartAt": self.valid_start_at.strftime("%Y-%m-%d %H:%M:%S"),
            "validEndAt": self.valid_end_at.strftime("%Y-%m-%d %H:%M:%S"),
            "userGroup": {
                "id": ug.id,
                "name": ug.name
            },
            "item": {
                "id": item.id,
                "name": item.name,
            },
            "total": self.redeem_set.count(),
            "used": self.redeem_set.where(Redeem.status == RedeemStatus.USED).count(),
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        }


class Redeem(BaseModel):
    "兑换码"
    id = pw.PrimaryKeyField()
    code = pw.CharField(unique=True, index=True)                # 兑换码
    activity = pw.ForeignKeyField(RedeemActivity)
    status = pw.IntegerField(default=RedeemStatus.UNUSE)        # 兑换状态
    user = pw.ForeignKeyField(User, null=True)                  # 所有者
    device = pw.ForeignKeyField(Device, null=True)
    created_at = pw.DateTimeField(default=dte.now)              # 创建时间
    use_at = pw.DateTimeField(null=True)                        # 使用时间

    class Meta:
        db_table = 'redeem'


class VoiceActivity(BaseModel):
    "口令活动"
    id = pw.PrimaryKeyField()
    code = pw.CharField()                                       # 口令码, 在有效期内是唯一的，由程序来维护它的唯一性。
    item = pw.ForeignKeyField(Item)
    device_group = pw.ForeignKeyField(DeviceGroup)
    valid_start_at = pw.DateTimeField()                         # 有效起始时间
    valid_end_at = pw.DateTimeField()                           # 有效截止时间
    limit = pw.IntegerField()                                   # 兑换上限
    created_at = pw.DateTimeField(default=dte.now)              # 创建时间

    class Meta:
        db_table = 'voice_activity'

    def to_dict(self):
        ug = self.device_group
        item = self.item
        return {
            "id": self.id,
            "validStartAt": self.valid_start_at.strftime("%Y-%m-%d %H:%M:%S"),
            "validEndAt": self.valid_end_at.strftime("%Y-%m-%d %H:%M:%S"),
            "code": self.code,
            "deviceGroup": {
                "id": ug.id,
                "name": ug.name
            },
            "item": {
                "id": item.id,
                "name": item.name,
            } if item else {},
            "total": self.voiceword_set.count(),
            "used": self.voiceword_set.where(VoiceWord.status == RedeemStatus.USED).count(),
            "limit": self.limit,
        }

    @property
    def name(self):
        return self.code


class VoiceWord(BaseModel):
    "口令"
    id = pw.PrimaryKeyField()
    activity = pw.ForeignKeyField(VoiceActivity)
    device = pw.ForeignKeyField(Device)
    status = pw.IntegerField(default=RedeemStatus.UNUSE)        # 兑换状态
    user = pw.ForeignKeyField(User, null=True)
    use_at = pw.DateTimeField(null=True)                        # 使用时间
    created_at = pw.DateTimeField(default=dte.now)              # 创建时间

    class Meta:
        db_table = 'voice_word'


class Order(BaseModel):
    """
    订单
    """

    id = pw.PrimaryKeyField()
    no = pw.CharField(unique=True)                          # 订单号 （唯一）
    road = pw.ForeignKeyField(Road, null=True)              # 出于哪个货道
    device = pw.ForeignKeyField(Device)                     # 出于哪个设备
    item_amount = pw.IntegerField(default=0)                # 商品数量
    item = pw.ForeignKeyField(Item)                         # 商品
    pay_money = pw.IntegerField(default=0)                  # 支付金额
    pay_status = pw.IntegerField()                          # 支付状态:
    pay_type = pw.IntegerField(null=True)                   # 支付类型： 支付宝&微信支付&兑换码
    pay_at = pw.DateTimeField(null=True)                    # 支付时间
    price = pw.IntegerField(default=0)                       # 订单金额
    redeem = pw.ForeignKeyField(Redeem, null=True)          # 兑换码
    voice_word = pw.ForeignKeyField(VoiceWord, null=True)   # 支付所使用的口令
    refund_money = pw.IntegerField(default=0)                 # 退款金额
    status = pw.IntegerField(null=True)                     # 订单状态
    user = pw.ForeignKeyField(User, null=True)              # 购买用户
    qrcode_url = pw.CharField(null=True)                    # 支付二维码链接
    deliver_at = pw.DateTimeField(null=True)
    created_at = pw.DateTimeField(default=dte.now)          # 创建时间

    class Meta:
        db_table = 'order'

    @classmethod
    def generate_order_no(self):
        """
        生成订单号, 6位日期+6位毫秒+2位随机数
        """
        now = dte.now()
        no = now.strftime("%y%m%d")
        no += "%06d" % now.microsecond
        no += str(random.randint(10, 99))
        return no


class SupplyList(BaseModel):
    """
    配货单
    """
    id = pw.PrimaryKeyField()
    no = pw.CharField(unique=True, index=True)
    device = pw.ForeignKeyField(Device)
    supplyer = pw.ForeignKeyField(Supplyer)
    status = pw.IntegerField(default=SupplyStatus.DOING, index=True)
    data_before = pw.CharField(max_length=2048)             # 更改前的数据
    data_after = pw.CharField(max_length=2048)              # 更改后
    done_at = pw.DateTimeField(null=True)
    created_at = pw.DateTimeField(default=dte.now)          # 创建时间

    class Meta:
        indexes = (
            (('device', 'status'), False),
        )

    @classmethod
    def generate_no(self):
        candidates = set(["%06d" % x for x in range(0, 10**6)])
        candidates -= set([t[0] for t in SupplyList.select(SupplyList.no).tuples()])
        return random.choice(list(candidates))

    def to_dict(self):
        device = self.device
        u = self.supplyer
        return{
            "id": self.id,
            "no": self.no,
            "status": self.status,
            "doneAt": self.done_at.strftime("%Y-%m-%d %H:%M:%S") if self.done_at else "",
            "device": {
                "id": device.id,
                "no": device.no,
                "name": device.name,
            },
            "supplyer": {
                "id": u.id,
                "mobile": u.mobile,
                "name": u.name,
            },
            "dataBefore": json.loads(self.data_before),
            "dataAfter": json.loads(self.data_after),
            "createdAt": self.created_at.strftime("%Y-%m-%d %H:%M:%S"),
        }


class SMSHistory(BaseModel):
    """
    短信发送记录
    """
    id = pw.PrimaryKeyField()
    status = pw.IntegerField(default=C.SMSStatus.READY)     # 发送状态
    mobile = pw.CharField(max_length=11)
    channel = pw.IntegerField(default=C.SMSChannel.NONE)    # 供应商
    content = pw.CharField(max_length=400, default="")      # 发送内容
    tplcode = pw.CharField()                                # 短信模板code
    tplparam = pw.CharField()                               # 模板参数
    biz_id = pw.CharField(default="")                       # 短信接口返回的回执id
    created_at = pw.DateTimeField(default=dte.now)          # 创建时间


class DayDeviceStat(BaseModel):
    """
    针对设备每天的统计数

    聚合表
    """
    day = pw.CharField(index=True)      # eg: 2019-09-12
    device = pw.ForeignKeyField(Device)
    flows = pw.IntegerField(default=0)                      # 人流量
    stays = pw.IntegerField(default=0)                      # 停留人数
    sales_volume = pw.IntegerField(default=0)                        # 销售额
    sales_quantity = pw.IntegerField(default=0)                      # 销售个数
    orders_pay = pw.IntegerField(default=0)                          # 支付笔数
    users_pay = pw.IntegerField(default=0)                           # 支付客户数
    item_clicks = pw.IntegerField(default=0)                         # 商品访问量
    item_visitors = pw.IntegerField(default=0)                       # 商品访客数
    clicks = pw.IntegerField(default=0)                              # 商品访问量
    visitors = pw.IntegerField(default=0)                            # 商品访客数
    created_at = pw.DateTimeField(default=dte.now)                   # 创建时间

    indexes = (
        (('day', 'device'), True),
    )


class DayItemStat(BaseModel):
    """
    针对商品每天的统计数

    聚合表
    """
    day = pw.CharField(index=True)                          # eg: 2019-09-12
    item = pw.ForeignKeyField(Item)
    sales_volume = pw.IntegerField(default=0)                        # 销售额
    sales_quantity = pw.IntegerField(default=0)                      # 销售个数
    orders_pay = pw.IntegerField(default=0)                          # 支付笔数
    users_pay = pw.IntegerField(default=0)                           # 支付客户数; 付款成功
    clicks = pw.IntegerField(default=0)                              # 商品访问量
    visitors = pw.IntegerField(default=0)                            # 商品访客数
    created_at = pw.DateTimeField(default=dte.now)                   # 创建时间

    indexes = (
        (('day', 'item'), True),
    )


class DayUserGroupStat(BaseModel):
    """
    针对用户群每天的统计数

    聚合表
    """
    day = pw.CharField(index=True)                          # eg: 2019-09-12
    user_group = pw.ForeignKeyField(UserGroup)
    users = pw.IntegerField(default=0)                      # 用户数
    registers = pw.IntegerField(default=0)                  # 用户增加量; 净增购买用户
    actives = pw.IntegerField(default=0)                    # 活跃用户
    sales_volume = pw.IntegerField(default=0)               # 销售额
    sales_quantity = pw.IntegerField(default=0)             # 销售个数
    orders_pay = pw.IntegerField(default=0)                 # 支付笔数
    users_pay = pw.IntegerField(default=0)                  # 支付用户数
    created_at = pw.DateTimeField(default=dte.now)          # 创建时间

    indexes = (
        (('day', 'user_group'), True),
    )


class DayStat(BaseModel):
    """
    每天数据汇总

    聚合表
    """
    day = pw.CharField(index=True, unique=True)             # eg: 2019-09-12
    users = pw.IntegerField(default=0)                      # 用户数
    registers = pw.IntegerField(default=0)                  # 用户增加量; 净增购买用户
    actives = pw.IntegerField(default=0)                    # 活跃用户
    sales_volume = pw.IntegerField(default=0)               # 销售额
    sales_quantity = pw.IntegerField(default=0)             # 销售个数
    orders_pay = pw.IntegerField(default=0)                 # 支付笔数
    users_pay = pw.IntegerField(default=0)                  # 支付客户数; 付款成功
    created_at = pw.DateTimeField(default=dte.now)          # 创建时间


MODEL_TABLES = [
    Image,
    Video,
    ADImage,
    ADVideo,
    AddressType,
    DeviceCategory,
    DeviceGroup,
    Supplyer,
    Admin,
    User,
    UserGroup,
    Advertisement,
    ApkVersion,
    Device,
    Road,
    Road.thumbnails.get_through_model(),
    RedeemActivity,
    Redeem,
    VoiceActivity,
    VoiceWord,
    ItemBrand,
    ItemCategory,
    Item,
    Item.thumbnails.get_through_model(),
    Order,
    SupplyList,
    SMSHistory,
    DayItemStat,
    DayDeviceStat,
    DayUserGroupStat,
    DayStat,
]


def create_tables():
    db.create_tables(MODEL_TABLES)


def drop_tables():
    db.drop_tables(MODEL_TABLES)
