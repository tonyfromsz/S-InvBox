
## Install RabbitMQ

### For OSX

    安装：brew install rabbitmq

    启动：/usr/local/sbin/rabbitmq-server 或者 brew services start rabbitmq

    登陆Web管理界面：localhost:15672，账号guest密码guest

### For Ubuntu


## Install m2crypto

### For OSX

    brew install openssl
    brew install swig
    sudo env LDFLAGS="-L$(brew --prefix openssl)/lib" \
CFLAGS="-I$(brew --prefix openssl)/include" \
SWIG_FEATURES="-cpperraswarn -includeall -I$(brew --prefix openssl)/include" \
pip install m2crypto

