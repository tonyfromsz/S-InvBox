# -*- coding: utf-8 -*-

import os
import os.path as osp
import sys


def autodiscover(filename, check=None):
    cur_dir = osp.abspath('.')
    if os.path.isfile(filename):
        abspath = osp.abspath(osp.dirname(filename))
    else:
        abspath = osp.abspath(filename)

    all_modules = []
    for path, dirs, files in os.walk(abspath):
        files = filter(lambda f: not f.startswith("__") and f.endswith(".py"), files)
        modules = [f[:-3] for f in files if not check or check(f)]
        if not modules:
            continue

        package_path = os.path.relpath(path, cur_dir)
        package = '.'.join([x for x in package_path.split(os.path.sep) if x != '.'])
        for m in modules:
            name = "%s.%s" % (package, m)
            __import__(name)
            all_modules.append(sys.modules[name])
    return all_modules
