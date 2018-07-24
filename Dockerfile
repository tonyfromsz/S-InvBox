FROM alpine:3.5

RUN apk add --no-cache python vim py-mysqldb musl-dev linux-headers g++ python-dev libxml2-dev libffi-dev\
    libxml2 libxslt libxslt-dev tzdata ca-certificates gfortran openblas-dev libressl-dev&& \
    rm -rf /var/cache/apk/* && \
    ln -s /usr/include/locale.h /usr/include/xlocale.h && \
    cp /usr/share/zoneinfo/Asia/Chongqing /etc/localtime && \
    python -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip install --upgrade pip setuptools && \
    rm -r /root/.cache

##############
ADD libs/ /src/libs/
WORKDIR /src/libs/aliyun-python-sdk-core
RUN python setup.py install
WORKDIR /src/libs/aliyun-python-sdk-dysmsapi
RUN python setup.py install

ADD requirements.txt /src/
WORKDIR /src
RUN pip install -r requirements.txt

ADD ./ /src/S-InvBox
WORKDIR /src/S-InvBox

RUN mkdir -p /src/logs && rm -rf tests/__pycache__
RUN cp config_sample.py config.py

CMD ["python", "manage.py", "runserver"]
