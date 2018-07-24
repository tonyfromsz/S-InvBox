# -*- coding: utf-8 -*-

import models as M
import random
import ujson as json

from util import md5
from const import OrderStatus, PayStatus
from datetime import datetime as dte, timedelta
from biz import MarktingBiz, DeviceBiz


def init_base():
    users = {
        "luke": "13064754229",
        "april": "13621713595",
    }

    for name, mobile in users.items():
        admin = M.Admin.create(
            mobile=mobile,
            username=name,
            password=md5(mobile[-6:]),
        )
        admin.save()

        user = M.User.create(
            mobile=mobile,
            username=name,
        )
        user.save()

    # apkversion
    apk = M.ApkVersion.create(
        version="1.0.0",
        up_type=1,
        url="/media/apk/smallbox.apk"
    )
    apk.save()


def init_all():
    init_base()
    return

    # 商品类别大图
    image_urls = ["/media/image/detail_0.png", "/media/image/detail_1.png"]
    dc_images = []
    for i in range(1, 7):
        image = M.Image.create(
            md5=md5(str(i)),
            url=random.choice(image_urls),
        )
        image.save()
        dc_images.append(image)

    # 商品类别缩略图
    image_urls = ["/media/image/s_detail_0.png", "/media/image/s_detail_1.png"]
    dc_thumbnails = []
    for i in range(7, 12):
        image = M.Image.create(
            md5=md5(str(i)),
            url=random.choice(image_urls),
        )
        image.save()
        dc_thumbnails.append(image)

    # 商品图
    item_images = []
    image_urls = [
        "/media/image/pdlt_pd_0.png",
        "/media/image/pdlt_pd_1.png",
        "/media/image/pdlt_pd_2.png",
        "/media/image/pdlt_pd_3.png",
    ]
    for i in range(12, 20):
        image = M.Image.create(
            md5=md5(str(i)),
            url=random.choice(image_urls),
        )
        image.save()
        item_images.append(image)

    # 广告图
    ad_images = []
    image_urls = [
        "/media/image/1.jpeg",
        "/media/image/2.jpg",
    ]
    for i in range(20, 25):
        image = M.Image.create(
            md5=md5(str(i)),
            url=random.choice(image_urls),
        )
        image.save()
        ad_images.append(image)

    # item category
    item_categories = []
    for i in range(1, 12):
        name = random.choice(["卫生巾", "卫生棉条"])
        name = "%s_%s" % (name, i)
        obj = M.ItemCategory.create(
            name=name,
            thumbnail=random.choice(dc_thumbnails),
            image=random.choice(dc_images),
        )
        obj.save()
        item_categories.append(obj)

    # item brand
    item_brands = []
    for i in range(1, 12):
        obj = M.ItemBrand.create(
            name="In-V-%s" % i,
        )
        obj.save()
        item_brands.append(obj)

    # item
    items = []
    for i in range(1, 12):
        price = random.randint(1, 4)
        cat = random.choice(item_categories)
        obj = M.Item.create(
            category=cat,
            brand=random.choice(item_brands),
            name="商品%s" % i,
            description="",
            basic_price=price,
            cost_price=price,
        )
        obj.save()
        obj.thumbnails.add([random.choice(item_images)])
        items.append(obj)

    # supplyer
    supplyers = []
    for i in range(1, 12):
        obj = M.Supplyer.create(
            name="supplyer-%s" % i,
            mobile="130246821%02d" % i,
        )
        obj.save()
        supplyers.append(obj)

    # device category
    device_categories = []
    for i in range(1, 12):
        cnt = random.randint(2, 6)
        obj = M.DeviceCategory.create(
            name="型号-%s" % i,
            road_count=cnt,
            road_meta_list=json.dumps([{"upper_limit": 30, "lower_limit": 3} for i in range(cnt)])
        )
        obj.save()
        device_categories.append(obj)

    # addresstype
    name_list = ["学校", "商场", "写字楼"]
    address_types = []
    for i in name_list:
        obj = M.AddressType.create(
            name=i,
        )
        obj.save()
        address_types.append(obj)

    # device
    involved_devices = []
    for i in range(1, 7):
        no = "1000%02d" % i if i != 1 else "123456"
        biz = DeviceBiz()
        device = biz.create(
            name="测试-%s" % i,
            no=no,
            address="深圳威新软件园-%s号楼" % i,
            involved=True,
            supplyer=random.choice(supplyers),
            category=random.choice(device_categories),
            address_type=random.choice(address_types),
        )
        device.save()
        involved_devices.append(device)

    for device in involved_devices:
        i = 0
        for r in device.road_set:
            i += 1
            r.item = items[i % 12]
            # r.price= r.item.basic_price
            r.save()

    uninvolved_devices = []
    for i in range(7, 12):
        biz = DeviceBiz()
        device = biz.create(
            no="1000%2d" % i, address="深圳威新软件园-%s号楼" % i,
            involved=False,
            category=random.choice(device_categories),
        )
        device.save()
        uninvolved_devices.append(device)

    # user
    users = []
    for i in range(1, 12):
        u = M.User.create(
            mobile="170254775%02d" % i,
            username="user-%s" % i,
        )
        u.save()
        users.append(u)

    # usergroup
    usergroups = []
    for i in range(1, 12):
        condition = []
        ug = M.UserGroup.create(
            name="测试组-%s" % i,
            condition=json.dumps(condition)
        )
        ug.save()
        usergroups.append(ug)

    # RedeemActivity
    items0 = map(lambda x: x[0],
                 list(involved_devices[0].road_set.select(M.Road.item).tuples()))
    biz = MarktingBiz()
    for i in range(1, 6):
        ra = biz.create_redeem_activity(
            "兑换码活动-%s" % i,
            random.choice(usergroups),
            dte.now(),
            dte.now() + timedelta(days=random.randint(5, 100)),
            random.choice(items0),
        )
        ra.save()

    # devicegroup
    devicegroups = []
    for i in range(1, 12):
        condition = []
        ug = M.DeviceGroup.create(
            name="测试组-%s" % i,
            condition=json.dumps(condition)
        )
        ug.save()
        devicegroups.append(ug)

    # voiceactivity
    biz = MarktingBiz()
    for i in range(1, 6):
        ra = biz.create_voice_activity(
            "%d号口令" % i,
            random.choice(devicegroups),
            dte.now(),
            dte.now() + timedelta(days=random.randint(5, 100)),
            100,
            random.choice(items),
        )
        ra.save()

    vidoes = []
    video_urls = [
        "/media/video/video1.mp4",
        "/media/video/video2.mp4",
        "/media/video/video3.mp4",
        "/media/video/video4.mp4",
    ]
    for i in range(1, 12):
        url = video_urls[i % 5 - 1]
        obj = M.Video.create(
            md5=md5(str(i) + url),
            url=url,
        )
        obj.save()
        vidoes.append(obj)

    # advideos
    advideos = []
    for i, v in enumerate(vidoes):
        obj = M.ADVideo.create(
            name="视屏%s" % (i + 1),
            video=v,
        )
        obj.save()
        advideos.append(obj)

    # adimage
    adimages = []
    for i, v in enumerate(ad_images):
        obj = M.ADImage.create(
            name="图片%s" % (i + 1),
            image=v,
        )
        obj.save()
        adimages.append(obj)

    da1 = M.Advertisement.create(
        device=involved_devices[0],
        a_video1=advideos[0],
        a_video2=advideos[1],
        a_video3=advideos[2],
        a_video4=advideos[3],
        a_text="这是一条很长的滚动广告\n这是第二条滚动广告\n这是第三条",
        b_image1=adimages[0],
        c_image1=adimages[1],
    )
    da1.save()

    for i in range(1, 12):
        order_no = M.Order.generate_order_no()
        device = random.choice(involved_devices)
        road = random.choice(list(device.road_set))
        amount = random.randint(1, 8)
        order = M.Order.create(
            no=order_no,
            road=road,
            device=device,
            item_amount=amount,
            item=road.item,
            price=road.sale_price * amount,
            pay_money=0,
            status=OrderStatus.CREATED,
            pay_status=PayStatus.UNPAY,
        )
        order.save()

    # supplylist
    for d in involved_devices[:4]:
        biz = DeviceBiz(device=d)
        list_no = M.SupplyList.generate_no()

        data_before = []
        data_after = []
        for o in d.road_set:
            data_before.append({
                "no": o.no,
                "item": {
                    "id": o.item.id,
                    "name": o.item.name,
                },
                "amount": o.amount,
            })

            data_after.append({
                "no": o.no,
                "item": {
                    "id": o.item.id,
                    "name": o.item.name,
                },
                "add": biz.get_road_upper_limit(o.no) - o.amount,
            })
        obj = M.SupplyList.create(no=list_no,
                                  device=d,
                                  supplyer=d.supplyer,
                                  data_before=json.dumps(data_before),
                                  data_after=json.dumps(data_after))
        obj.save()

    now = dte.now()
    for i in range(-100, 1):
        time = now + timedelta(days=i)
        day = time.strftime("%Y-%m-%d")
        for it in items:
            obj = M.DayItemStat.create(
                day=day,
                item=it,
                sales_volume=100,
                sales_quantity=5,
                orders_pay=2,
                users_pay=2,
                clicks=100,
                visitors=80,
            )
            obj.save()

    now = dte.now()
    for i in range(-100, 1):
        time = now + timedelta(days=i)
        day = time.strftime("%Y-%m-%d")
        for it in involved_devices:
            obj = M.DayDeviceStat.create(
                day=day,
                device=it,
                flows=200,
                stays=200,
                sales_volume=100,
                sales_quantity=5,
                orders_pay=2,
                users_pay=2,
                clicks=101,
                visitors=81,
                item_clicks=100,
                item_visitors=80,
            )
            obj.save()

    now = dte.now()
    for i in range(-100, 1):
        time = now + timedelta(days=i)
        day = time.strftime("%Y-%m-%d")
        for it in usergroups:
            obj = M.DayUserGroupStat.create(
                day=day,
                user_group=it,
                users=200,
                registers=2,
                actives=10,
                sales_volume=100,
                sales_quantity=5,
                orders_pay=2,
                users_pay=2,
            )
            obj.save()

    now = dte.now()
    for i in range(-100, 1):
        time = now + timedelta(days=i)
        day = time.strftime("%Y-%m-%d")
        obj = M.DayStat.create(
            day=day,
            users=10000,
            registers=12,
            actives=100,
            sales_volume=9000,
            sales_quantity=82,
            orders_pay=80,
            users_pay=60,
        )
        obj.save()
