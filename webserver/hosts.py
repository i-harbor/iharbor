from django.conf import settings
from django_hosts import patterns, host


use_s3_api = getattr(settings, 'USE_S3_API', True)


def get_s3_sub_host_names():
    main_hosts = getattr(settings, 'S3_SERVER_HTTP_HOST_NAME', [])
    if not main_hosts:
        raise Exception('开启使用S3兼容接口，必须设置S3接口的域名，通过配置参数"S3_SERVER_HTTP_HOST_NAME"指定。')

    sub_hosts = []
    for main_host in main_hosts:
        if not isinstance(main_host, str):
            raise Exception('开启使用S3兼容接口，通过配置参数"S3_SERVER_HTTP_HOST_NAME"指定S3接口的域名必须是字符串格式。')

        main_host = main_host.lower()
        items = main_host.rsplit('.', maxsplit=2)
        length = len(items)
        if length != 3:
            raise Exception('开启使用S3兼容接口，通过配置参数"S3_SERVER_HTTP_HOST_NAME"指定S3接口的域名不得小于3级域名，x.x.x。')

        sub_host = items[0]
        if sub_host not in sub_hosts:
            sub_hosts.append(sub_host)

    return sub_hosts


def get_hosts():
    _hosts = [host(r'obs', settings.ROOT_URLCONF, name='default')]
    if use_s3_api:
        sub_hosts = get_s3_sub_host_names()
        for sub in sub_hosts:
            _hosts.append(host(sub, 's3.urls', name=f's3-{sub}'))
            _hosts.append(host(r'([\w-]+).' + sub, 's3.sub_urls', name=f's3-sub-{sub}'))

    return _hosts


hosts = get_hosts()
host_patterns = patterns(
    '', *hosts
    # host(r'obs', settings.ROOT_URLCONF, name='default'),
    # host(r's3.obs', 's3.urls', name='s3'),
)
