# -*- coding: utf-8 -*-
import hashlib

from lxml import etree
from concurrent.futures import ThreadPoolExecutor

_executor = ThreadPoolExecutor(4)


def md5(text):
    m = hashlib.md5()
    m.update(text)
    return m.hexdigest()


def xml_to_dict(content):
    raw = {}
    root = etree.fromstring(content)
    for child in root:
        raw[child.tag] = child.text
    return raw


def dict_to_xml(data):
    s = ""
    for k, v in data.items():
        s += "<{0}>{1}</{0}>".format(k, v)
    s = "<xml>{0}</xml>".format(s)
    return s.encode("utf-8")


def thread_call_out(func, *args, **kwargs):
    _executor.submit(func, *args, **kwargs)
