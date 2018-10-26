import math

from django import template
from django.template.defaultfilters import stringfilter

register = template.Library()

@register.filter(name='to_str')
def to_str(value):
    '''自定义转字符串过滤器'''
    return str(value)


@register.filter(name='name_from_path')
@stringfilter
def name_from_path(path):
    '''从路径中截取文件名或最低级目录名'''
    if not path:
        return ''
    l = path.split('/')
    r = l[-1]
    if r:
        return r
    return l[-2]


@register.filter(name='format_size')
def format_size(size):
    '''
    文件大小字节数转换为易读KB\MB\GB格式
    :param size:
    :return:
    '''
    if 0 <= size < 1024**3:
        return f'{math.ceil(size/1024)} KB'
    elif size < 1024**4:
        return f'{(size/(1024**3)):.4} GB'
    else:
        return f'{(size/(1024**4)):.4} TB'


