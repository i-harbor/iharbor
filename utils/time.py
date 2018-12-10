from pytz import utc

from django.utils import timezone


def time_to_local_naive_by_utc(value=None):
    '''
    时间转本地时间,如果无时区信息默认按utc时间

    :param time: datetime对象
    :return: datetime对象
    '''
    if value is None:
        value = timezone.now()

    # 无时区信息按UTC
    if timezone.is_naive(value):
        value = value.replace(tzinfo=utc)

    return timezone.localtime(value)

def to_localtime_string_naive_by_utc(value, fmt="%Y-%m-%d %H:%M:%S"):
    '''
    时间转本地时间字符串,如果无时区信息默认按utc时间

    :param value: datetime对象
    :param fmt: 时间格式字符串
    :return: 错误返回空字符串
    '''
    try:
        time = time_to_local_naive_by_utc(value).strftime(fmt)  # 本地时区时间
    except:
        time = ''
    return time

