"""
Django settings for webserver project.

Generated by 'django-admin startproject' using Django 1.11.

For more information on this file, see
https://docs.djangoproject.com/en/1.11/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.11/ref/settings/
"""

import os
import sys
import datetime


# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.insert(0, os.path.join(BASE_DIR, 'apps'))

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.11/howto/deployment/checklist/

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = False

ALLOWED_HOSTS = ['*',]
INTERNAL_IPS = []
# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

    # 第三方apps
    'rest_framework',
    'rest_framework.authtoken',
    'corsheaders',
    'drf_yasg',
    'ckeditor',
    # 'ckeditor_uploader',

    #自定义apps
    'buckets.apps.BucketsConfig',
    'users.apps.UsersConfig',
    'api',
    'evcloud',
    'docs',
    'vpn',
    'share',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.locale.LocaleMiddleware',
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

# 允许所有主机执行跨站点请求
CORS_ORIGIN_ALLOW_ALL = True

# 有权发出跨站点HTTP请求的源主机名列表
# CORS_ORIGIN_WHITELIST  =(
#     'localhost:8000 ',
#     '10.0.86.213:8000',
# )

ROOT_URLCONF = 'webserver.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [
            os.path.join(BASE_DIR, 'templates'),
        ],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.i18n',
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'webserver.wsgi.application'


# Password validation
# https://docs.djangoproject.com/en/1.11/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/1.11/topics/i18n/
LANGUAGES = (
    ('en', 'English'),
    ('zh-hans', '中文简体'),
)

# 翻译文件所在目录
LOCALE_PATHS = (
    os.path.join(BASE_DIR, 'locale'),
)

LANGUAGE_CODE = 'zh-hans'

TIME_ZONE = 'Asia/Shanghai'

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.11/howto/static-files/

STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'collect_static')
#静态文件查找路径
STATICFILES_DIRS = [
    os.path.join(BASE_DIR, 'static'),
]

#上传文件
MEDIA_URL = '/media/'
MEDIA_ROOT = os.path.join(BASE_DIR, "media") 

#session 有效期设置
SESSION_SAVE_EVERY_REQUEST = True #
SESSION_EXPIRE_AT_BROWSER_CLOSE = True #True：关闭浏览器，则Cookie失效。
# SESSION_COOKIE_AGE=60*30   #30分钟

#自定义用户模型
AUTH_USER_MODEL = 'users.UserProfile'

# 避免django把未以/结尾的url重定向到以/结尾的url
APPEND_SLASH=False

#登陆url
LOGIN_URL = '/users/signin/'
LOGOUT_URL = '/users/logout/'


REST_FRAMEWORK = {
    # Use Django's standard `django.contrib.auth` permissions,
    # or allow read-only access for unauthenticated users.
    'DEFAULT_PERMISSION_CLASSES': [
        # 'rest_framework.permissions.IsAuthenticatedOrReadOnly',
        'rest_framework.permissions.IsAuthenticated',
        # 'rest_framework.permissions.DjangoModelPermissionsOrAnonReadOnly',
    ],
    'DEFAULT_AUTHENTICATION_CLASSES': (
        'users.auth.authentication.AuthKeyAuthentication',
        'rest_framework.authentication.TokenAuthentication',
        'rest_framework_simplejwt.authentication.JWTAuthentication',
        'rest_framework.authentication.BasicAuthentication',
        'rest_framework.authentication.SessionAuthentication',
    ),
    'DEFAULT_PARSER_CLASSES': (
        'rest_framework.parsers.JSONParser', # 支持解析application/json方式的json数据
        'rest_framework.parsers.FormParser', # 支持解析application/x-www-form-urlencoded方式的form表单数据，request.data将填充一个QueryDict
        'rest_framework.parsers.MultiPartParser' # 支持解析multipart/form-data方式多部分HTML表单内容，支持文件上载，request.data将填充一个QueryDict
    ),
    # 'DEFAULT_THROTTLE_CLASSES': (
    #     'rest_framework.throttling.AnonRateThrottle',  # 未登陆认证的用户默认访问限制
    #     'rest_framework.throttling.UserRateThrottle'  # 登陆认证的用户默认访问限制
    # ),
    # 'DEFAULT_THROTTLE_RATES': {
    #     'anon': '5/minute',  # 未登陆认证的用户默认请求访问限制每分钟次数
    #     'user': '20/minute'  # 登陆认证的用户默认请求访问限制每分钟次数
    # },
    # api version settings
    # 'DEFAULT_VERSIONING_CLASS': 'rest_framework.versioning.URLPathVersioning',
    # 'DEFAULT_VERSION': 'v1',
    # 'ALLOWED_VERSIONS': ('v1', ),
    # 'VERSION_PARAM': 'version',

    # 分页
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.LimitOffsetPagination',
    'PAGE_SIZE': 100,

    'DEFAULT_SCHEMA_CLASS': 'rest_framework.schemas.coreapi.AutoSchema',
}


SIMPLE_JWT = {
    'ACCESS_TOKEN_LIFETIME': datetime.timedelta(hours=8),
    'REFRESH_TOKEN_LIFETIME': datetime.timedelta(days=2),
    'ROTATE_REFRESH_TOKENS': False, # True时，refresh API会返回内容中会包含一个新的refresh JWT
    'BLACKLIST_AFTER_ROTATION': True,

    # 'SIGNING_KEY': 'xxxxx',   # 默认SECRET_KEY

    'AUTH_HEADER_TYPES': ('Bearer',), # Header "Authorization:{AUTH_HEADER_TYPES} xxx"
    'USER_ID_FIELD': 'id',
    'USER_ID_CLAIM': 'user_id',

    'AUTH_TOKEN_CLASSES': ('rest_framework_simplejwt.tokens.AccessToken',),
}

# Ceph rados settings
CEPH_RADOS = {
    'CLUSTER_NAME': 'ceph',
    'USER_NAME': 'client.obs',
    'CONF_FILE_PATH': '/etc/ceph/ceph.conf',
    'KEYRING_FILE_PATH': '/etc/ceph/ceph.client.obs.keyring',
    'POOL_NAME': ('obs',),
}

# 日志配置
LOGGING_FILES_DIR = '/var/log/iharbor'
if not os.path.exists(LOGGING_FILES_DIR):
    os.makedirs(LOGGING_FILES_DIR, exist_ok=True)

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'simple': {
            'format': '%(levelname)s %(message)s'
        },
        'dubug_formatter': {
            'format': '%(levelname)s %(asctime)s %(message)s'
        },
    },
    'filters': {
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse',
        },
        'require_debug_true': {
            '()': 'django.utils.log.RequireDebugTrue',
        },
    },
    'handlers': {
        # logging file settings
        'file': {
            'level': 'WARNING',
            'class': 'concurrent_log_handler.ConcurrentRotatingFileHandler',
            'filename': os.path.join(LOGGING_FILES_DIR, 'iharbor.log'),
            'formatter': 'verbose',
            'maxBytes': 1024*1024*200,  # 200MB
            'backupCount': 10           # 最多10个文件
        },
        # output to console settings
        'console': {
            'level': 'DEBUG',
            'filters': ['require_debug_true'],# working with debug mode
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        # debug logging file settings
        'debug': {
            'level': 'DEBUG',
            'filters': ['require_debug_false'],# working with debug mode
            'class': 'concurrent_log_handler.ConcurrentRotatingFileHandler',
            'filename': os.path.join(LOGGING_FILES_DIR, 'debug.log'),
            'formatter': 'dubug_formatter',
            'maxBytes': 1024*1024*200,  # 200MB
            'backupCount': 10           # 最多10个文件
        },
        # 邮件通知
        # 'mail_admins': {
        #     'level': 'ERROR',
        #     'class': 'django.utils.log.AdminEmailHandler',
        #     'filters': ['special']
        # }
    },
    'loggers': {
        'django': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True,
        },
        'django.request': {
            'handlers': ['file', 'console'],#'mail_admins'
            'level': 'ERROR',
            'propagate': False,
        },
        'debug': {
            'handlers': ['debug'],
            'level': 'WARNING',#'DEBUG',
            'propagate': False,
        },
        # 'django.db.backends': {
        #     'handlers': ['console'],
        #     'propagate': True,
        #     'level':'DEBUG',
        # },
    },
}

# ckeditor
# CKEDITOR_UPLOAD_PATH = "upload/"
CKEDITOR_CONFIGS = {
    'default': {
        'skin': 'moono',
        # 'skin': 'office2013',
        'toolbar_Basic': [
            ['Source', '-', 'Bold', 'Italic']
        ],
        'toolbar_YourCustomToolbarConfig': [
            {'name': 'document', 'items': ['Source', '-', 'Save', 'NewPage', 'Preview', 'Print', '-', 'Templates']},
            {'name': 'clipboard', 'items': ['Cut', 'Copy', 'Paste', 'PasteText', 'PasteFromWord', '-', 'Undo', 'Redo']},
            {'name': 'editing', 'items': ['Find', 'Replace', '-', 'SelectAll']},
            {'name': 'forms',
             'items': ['Form', 'Checkbox', 'Radio', 'TextField', 'Textarea', 'Select', 'Button', 'ImageButton',
                       'HiddenField']},
            '/',
            {'name': 'basicstyles',
             'items': ['Bold', 'Italic', 'Underline', 'Strike', 'Subscript', 'Superscript', '-', 'RemoveFormat']},
            {'name': 'paragraph',
             'items': ['NumberedList', 'BulletedList', '-', 'Outdent', 'Indent', '-', 'Blockquote', 'CreateDiv', '-',
                       'JustifyLeft', 'JustifyCenter', 'JustifyRight', 'JustifyBlock', '-', 'BidiLtr', 'BidiRtl',
                       'Language']},
            {'name': 'links', 'items': ['Link', 'Unlink', 'Anchor']},
            {'name': 'insert',
             'items': ['Image', 'Flash', 'Table', 'HorizontalRule', 'Smiley', 'SpecialChar', 'PageBreak', 'Iframe']},
            '/',
            {'name': 'styles', 'items': ['Styles', 'Format', 'Font', 'FontSize']},
            {'name': 'colors', 'items': ['TextColor', 'BGColor']},
            {'name': 'tools', 'items': ['Maximize', 'ShowBlocks']},
            {'name': 'about', 'items': ['About']},
            '/',  # put this to force next toolbar on new line
            {'name': 'yourcustomtools', 'items': [
                # put the name of your editor.ui.addButton here
                'Preview',
                'Maximize',
            ]},
        ],
        'toolbar': 'YourCustomToolbarConfig',  # put selected toolbar config here
        'tabSpaces': 4,
    },
    'custom': {
        'toolbar': 'Custom',
        'toolbar_Custom': [
            {'name': 'styles', 'items': ['Format', 'Bold', 'Italic', 'Underline']},
            {'name': 'paragraph',
             'items': ['NumberedList', 'Outdent', 'Indent', 'JustifyLeft',
                       'JustifyCenter', 'JustifyRight', 'JustifyBlock']},
            {'name': 'colors', 'items': ['TextColor', 'BGColor']},
            {'name': 'insert', 'items': ['Link', 'Unlink', 'Image', 'Smiley', 'SpecialChar']},
            {'name': 'tools',
             'items': ['RemoveFormat', 'Undo', 'Redo', 'SelectAll', 'Maximize', 'Source']},
        ],
        # 配置上传图片时不需要的内联样式属性
        'disallowedContent': 'img{width,height,margin-left, margin-right};img[width,height,margin-left, margin-right];'
    },
}


DATABASE_ROUTERS = [
    'webserver.db_routers.MetadataRouter',
]


# 第三方应用登录认证
THIRD_PARTY_APP_AUTH = {
    # 科技云通行证
    'SCIENCE_CLOUD': {
        # 'client_id': 000,
        # 'client_secret': 'xxx',
        'client_home_url': 'http://obs.cstcloud.cn',
        'client_callback_url': 'http://obs.cstcloud.cn/callback/', # 认证回调地址
        'login_url': 'https://passport.escience.cn/oauth2/authorize?response_type=code&theme=embed',
        'token_url': 'https://passport.escience.cn/oauth2/token',
        'logout_url': 'https://passport.escience.cn/logout'
    },
}

# drf-yasg
SWAGGER_SETTINGS = {
    # 'LOGIN_URL': reverse_lazy('admin:login'),
    # 'LOGOUT_URL': '/admin/logout',
    'PERSIST_AUTH': True,
    'REFETCH_SCHEMA_WITH_AUTH': True,
    'REFETCH_SCHEMA_ON_LOGOUT': True,


    'SECURITY_DEFINITIONS': {
        'Basic': {
            'type': 'basic'
        },
        'Bearer': {
            'in': 'header',
            'name': 'Authorization',
            'type': 'apiKey',
        }
    },
}

# 自定义文件上传处理文件大小限制, type: int
CUSTOM_UPLOAD_MAX_FILE_SIZE = 10 * 2**30  # 10GB; None: 无限制

# 导入安全相关的settings
from .security_settings import *

# The following is examples for security_settings.py

# SECURITY WARNING: keep the secret key used in production secret!
# SECRET_KEY = 'tbfpk*ax#48#^_qzr-cg07&z9&+8j68=x41w5lzv^wsv7xax=v'

# # Database
# # https://docs.djangoproject.com/en/1.11/ref/settings/#databases
#
# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.mysql',   # 数据库引擎
#             'NAME': 'xxx',       # 数据的库名，事先要创建之
#             'USER': 'xxx',         # 数据库用户名
#             'PASSWORD': 'xxx',     # 密码
#             'HOST': '0.0.0.0',    # 主机
#             'PORT': '3306',         # 数据库使用的端口
#         'CONN_MAX_AGE': 3600,   # 1h, None用于无限的持久连接
#         'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"}
#     },
#     'metadata': {
#         'ENGINE': 'django.db.backends.mysql',  # 数据库引擎
#         'NAME': 'xxx',  # 数据的库名，事先要创建之
#         'USER': 'xxx',  # 数据库用户名
#         'PASSWORD': 'xxx',  # 密码
#         'HOST': '0.0.0.0',  # 主机
#         'PORT': '3306',  # 数据库使用的端口
#         'CONN_MAX_AGE': 3600,   # 1h, None用于无限的持久连接
#         'OPTIONS': {'init_command': "SET sql_mode='STRICT_TRANS_TABLES'"}
#     },
# }

# 邮箱配置
# EMAIL_BACKEND = 'django.core.mail.backends.smtp.EmailBackend'
# EMAIL_USE_TLS = True   #是否使用TLS安全传输协议
# # EMAIL_PORT = 25
# EMAIL_HOST = 'xxx'
# EMAIL_HOST_USER = 'xxx'
# EMAIL_HOST_PASSWORD = 'xxx'

# RAVEN_CONFIG = {
#     'dsn': 'sentry上面创建项目的时候得到的dsn'
# }

# 第三方应用登录认证敏感信息
# THIRD_PARTY_APP_AUTH_SECURITY = {
#     # 科技云通行证
#     'SCIENCE_CLOUD': {
#         'client_id': 000,
#         'client_secret': 'xxx',
#     },
# }

if DEBUG:
    # django debug toolbar
    INSTALLED_APPS.append('debug_toolbar')
    MIDDLEWARE.append('debug_toolbar.middleware.DebugToolbarMiddleware')
    DEBUG_TOOLBAR_CONFIG = {
        # 'SHOW_COLLAPSED': True,
    }
    INTERNAL_IPS += ['159.226.50.246', '127.0.0.1'] # 通过这些IP地址访问时，页面才会出现django debug toolbar面板
