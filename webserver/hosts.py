from django.conf import settings
from django_hosts import patterns, host

host_patterns = patterns(
    '',
    host(r'obs', settings.ROOT_URLCONF, name='default'),
    host(r's3.obs', 's3.urls', name='s3'),
    host(r'([\w-]+).s3.obs', 's3.sub_urls', name='sub_s3'),
)
