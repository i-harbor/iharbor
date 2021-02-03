from pytz import utc
from datetime import datetime

from django.utils import timezone
from django.utils.timezone import utc
from rest_framework import serializers

GMT_FORMAT = '%a, %d %b %Y %H:%M:%S GMT'


def time_to_gmt(value):
    """
    :param value: datetime()
    :return:
        GMT time string
    """
    try:
        return serializers.DateTimeField(format=GMT_FORMAT, default_timezone=utc).to_representation(value)
    except Exception as e:
        return ''


def datetime_from_gmt(value):
    """
    :param value: gmt格式时间字符串
    :return:
        datetime() or None
    """
    try:
        t = datetime.strptime(value, GMT_FORMAT)
        return t.replace(tzinfo=utc)
    except Exception as e:
        return None


def time_to_local_naive_by_utc(value=None):
    '''
    时间转本地时间,如果无时区信息默认按utc时间

    :param value: datetime对象
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


def to_django_timezone(value):
    """
    转换datetime到django设置的当前时区

    :return:
        若django未设置时区， 返回不带时区的datetime
        若django设置时区， 返回带时区的datetime
        如转换错误，返回原datetime
    """
    ctz = timezone.get_current_timezone()

    if ctz is not None:
        if timezone.is_aware(value):
            try:
                return value.astimezone(ctz)
            except OverflowError as e:
                return value
        try:
            return timezone.make_aware(value, ctz)
        except ValueError as e:
            return value
    elif (ctz is None) and timezone.is_aware(value):
        return timezone.make_naive(value, utc)
    return value

