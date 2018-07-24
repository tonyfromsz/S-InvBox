# -*- coding: utf-8 -*-
"""

"""
import pytest
import logging

from nameko.testing.services import worker_factory
from service.service import InvboxService
from models import create_tables, drop_tables
from tests import init_data

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def detect_fail_status(request):
    yield
    if int(request.session.testsfailed):
        pytest.invbox_failed = True


@pytest.fixture(scope="module")
def invbox():
    service = worker_factory(InvboxService)
    return service


# 默认是scode=”function”，表示fixture会为每个function初始化一次。
@pytest.fixture(scope="module", autouse=True)
def init_db(request):
    create_tables()
    init_data.init_all()

    def fin():  # 销毁函数, 测试函数退出时执行
        drop_tables()
    request.addfinalizer(fin)


def test_login(invbox):
    """
    测试登录
    """
    res = invbox.check_login("test01", "naorobot")
    assert res["resultCode"] == 0

    # 测试手机号登录
    res = invbox.check_login("13064754229", "naorobot")
    assert res["resultCode"] == 0

    res = invbox.check_login("test01", "")
    assert res["resultCode"] > 0

    res = invbox.check_login("test01", "11111")
    assert res["resultCode"] > 0

    res = invbox.check_login("", "naorobot")
    assert res["resultCode"] > 0


def test_admin_base(invbox):
    """
    测试管理员的增删改查
    """
    res = invbox.get_admin(1)
    assert res["resultCode"] == 0

    res = invbox.get_admin(0)
    assert res["resultCode"] > 0

    res = invbox.get_admin("1")
    assert res["resultCode"] == 0


def test_supplyer(invbox):
    """
    测试补货员的增删改查
    测试分页
    测试事务
    """

    adds = []
    for i in range(11):
        name = "test%02d" % i
        telephone = "155751013%02d" % i
        res = invbox.add_supplyer(name, telephone)
        assert res["resultCode"] == 0
        assert res["name"] == name
        assert res["mobile"] == telephone
        adds.append(res)

    # 测试重复添加
    res = invbox.add_supplyer(name, telephone)
    assert res["resultCode"] > 0

    # 测试分页
    res = invbox.get_supplyers(page=1, page_size=6)
    assert res["page"] == 1
    assert res["pageSize"] == 6
    assert res["totalCount"] > 11
    assert len(res["items"]) == 6
    res = invbox.get_supplyers(page=2, page_size=6)
    assert len(res["items"]) == 6

    res = invbox.modify_supplyers([
        {"id": adds[0]["id"], "name": adds[0]["name"] + "modify", "mobile": adds[0]["mobile"]},
        {"id": adds[1]["id"], "name": adds[1]["name"], "mobile": "13064754339"},
    ])
    assert res["resultCode"] == 0

    res = invbox.get_supplyers(ids=[adds[0]["id"], adds[1]["id"]])
    assert len(res["items"]) == 2
    assert res["items"][0]["name"].endswith("modify")
    assert res["items"][1]["name"] == adds[1]["name"]
    assert res["items"][0]["mobile"] == adds[0]["mobile"]
    assert res["items"][1]["mobile"] == "13064754339"
