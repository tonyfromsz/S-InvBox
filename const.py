# -*- coding: utf-8 -*-


class Enum:
    pass


PAY_EXPIRE_SECONDS = 60 * 3         # 支付失效时间
DELIVER_EXPIRE_SECONDS = 60 * 2     # 出货时间；过了2分钟不出货视为出货失败
WAITING_PAY_EXPIRE_SECONDS = 2 * 60 + 30   # 等待支付超时；超过这个时间则关闭订单


# 货道状态
RoadStatus = Enum()
RoadStatus.FAULT = 1             # 停售
RoadStatus.SELLING = 10            # 正常出售

RoadStatusMsg = {
    RoadStatus.FAULT: "故障",
    RoadStatus.SELLING: "正常",
}

# 订单状态
OrderStatus = Enum()
OrderStatus.CREATED = 1             # 等待付款
OrderStatus.DELIVERING = 2          # 已付款；出货中
OrderStatus.DONE = 3                # 订单成功；出货成功
OrderStatus.CLOSED = 10             # 订单关闭；订单失效;
OrderStatus.DELIVER_FAILED = 11     # 出货失败；
OrderStatus.REFUNDED = 13           # 退款完成
OrderStatus.DELIVER_TIMEOUT = 15    # 出货超时

OrderStatusMsg = {
    OrderStatus.CREATED: "等待付款",
    OrderStatus.DELIVERING: "出货中",
    OrderStatus.DONE: "出货成功",
    OrderStatus.CLOSED: "订单失效",
    OrderStatus.DELIVER_FAILED: "出货失败",
    OrderStatus.REFUNDED: "已退款",
    OrderStatus.DELIVER_TIMEOUT: "出货超时",
}


# 支付状态
PayStatus = Enum()
PayStatus.UNPAY = 1             # 未支付
PayStatus.PAIED = 2             # 已支付
PayStatus.REFUND = 3            # 已退款
PayStatus.CLOSED = 4            # 关闭


# 支付类型
PayType = Enum()
PayType.WX = 1              # 微信支付
PayType.ALIPAY = 2          # 支付宝
PayType.REDEEM = 3          # 兑换码
PayType.VOICE = 4           # 语音口令

PayTypeMsg = {
    PayType.WX: "微信",
    PayType.ALIPAY: "支付宝",
    PayType.REDEEM: "兑换码",
    PayType.VOICE: "兑换口令",
}


# 兑换状态
RedeemStatus = Enum()
RedeemStatus.UNUSE = 1      # 未使用
RedeemStatus.USED = 2       # 已使用

RedeemStatusMsg = {
    RedeemStatus.UNUSE: "未使用",
    RedeemStatus.USED: "已使用",
}


# 故障类型
FaultType = Enum()
FaultType.NONE = 0              # 无故障
FaultType.DELIVER_ERROR = 1     # 出货异常


FaultMsg = {
    FaultType.NONE: "正常",
    FaultType.DELIVER_ERROR: "出货异常"
}


# 配货状态
SupplyStatus = Enum()
SupplyStatus.DOING = 1
SupplyStatus.DONE = 2

SupplyStatusMsg = {
    SupplyStatus.DOING: "配货中",
    SupplyStatus.DONE: "已完成",
}

# 短信发送状态
SMSStatus = Enum()
SMSStatus.READY = 0
SMSStatus.FAIL = 1
SMSStatus.OK = 2

# 短信渠道
SMSChannel = Enum()
SMSChannel.NONE = 0     # 未知
SMSChannel.ALI = 1      # 阿里
