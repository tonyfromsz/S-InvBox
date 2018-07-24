#!/usr/bin/env python
# encoding: utf-8

from fabric.api import env, settings
from fabric.operations import sudo


def deploy():
    with settings(warn_only=True):
        sudo("sudo docker rm -f `docker ps -a -f 'name=invbox-service' -q`")
    sudo("sudo docker login -u %s -p %s %s" % (env.dkuser, env.dkpwd, env.image))
    sudo("sudo docker pull %s" % env.image)
    sudo("""sudo docker run --detach \
    --name invbox-service \
    --restart always \
    --volume /tmp:/src/logs \
    %s
""" % env.image)
