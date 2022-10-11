from .settings import PASSPORT_JWT


# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'tbfpk*ax#48#^_qzr-cg07&z9&+8j68=x41w5lzv^wsv7xax=v'

# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.mysql',   # 数据库引擎
        'NAME': 'xxx',       # 数据的库名，事先要创建之
        'USER': 'xxx',         # 数据库用户名
        'PASSWORD': 'xxx',     # 密码
        'HOST': '0.0.0.0',    # 主机
        'PORT': '3306',         # 数据库使用的端口
        'CONN_MAX_AGE': 3600,   # 1h, None用于无限的持久连接
        'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"}
    },
    'metadata': {
        'ENGINE': 'django.db.backends.mysql',  # 数据库引擎
        'NAME': 'xxx',  # 数据的库名，事先要创建之
        'USER': 'xxx',  # 数据库用户名
        'PASSWORD': 'xxx',  # 密码
        'HOST': '0.0.0.0',  # 主机
        'PORT': '3306',  # 数据库使用的端口
        'CONN_MAX_AGE': 3600,   # 1h, None用于无限的持久连接
        'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"}
    },
}

# 邮箱配置
EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
EMAIL_USE_TLS = True   # 是否使用TLS安全传输协议
# EMAIL_PORT = 25
EMAIL_HOST = 'xxx'
EMAIL_HOST_USER = 'xxx'
EMAIL_HOST_PASSWORD = 'xxx'

RAVEN_CONFIG = {
    'dsn': 'sentry上面创建项目的时候得到的dsn'
}

# 第三方应用登录认证敏感信息
THIRD_PARTY_APP_AUTH_SECURITY = {
    # 科技云通行证
    'SCIENCE_CLOUD': {
        'client_id': 000,
        'client_secret': 'xxx',
    },
}

# Ceph rados settings 修改成动态更新
# CEPH_RADOS = {
#     'default': {
#         'CLUSTER_NAME': 'ceph',
#         'USER_NAME': 'client.admin',
#         'CONF_FILE_PATH': '/etc/ceph/ceph.conf',
#         'KEYRING_FILE_PATH': '/etc/ceph/ceph.client.admin.keyring',
#         'POOL_NAME': ('xxx',),
#         'DISABLE_CHOICE': False,                # True: 创建bucket时不选择；
#     },
#     'xxx': {
#         'CLUSTER_NAME': 'ceph',
#         'USER_NAME': 'client.obs',
#         'CONF_FILE_PATH': '/etc/ceph/ceph2.conf',
#         'KEYRING_FILE_PATH': '/etc/ceph/ceph2.client.obs.keyring',
#         'POOL_NAME': ('xxx',),
#         'DISABLE_CHOICE': True,               # True: 创建bucket时不选择；
#     }
# }


# 允许所有主机执行跨站点请求
CORS_ORIGIN_ALLOW_ALL = True

# 有权发出跨站点HTTP请求的源主机名列表
# CORS_ORIGIN_WHITELIST = (
#     'http://127.0.0.1:8000',
# )
# CORS_ORIGIN_REGEX_WHITELIST = [
#     # r"^https://\w+\.example\.com$",
# ]

PASSPORT_JWT['VERIFYING_KEY'] = """
-----BEGIN PUBLIC KEY-----
xxx
-----END PUBLIC KEY-----
"""

# test case settings
TEST_CASE_SECURITY = {
    'BACKUP_BUCKET': {
        'endpoint_url': 'http://127.0.0.1/',
        'bucket_name': 'xxx',
        'bucket_token': 'xxx'
    }
}
