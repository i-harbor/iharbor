import re

from rest_framework.serializers import ValidationError

from .models import Bucket, BucketLimitConfig


dns_regex = re.compile(r'(?!-)' # can't start with a -
                       r'[a-zA-Z0-9-]{,63}'
                       r'(?<!-)$')  # can't end with a dash

#'^[a-zA-Z0-9][-a-zA-Z0-9]{0,63}(?<!-)'

def DNSStringValidator(value):
    '''
    验证字符串是否符合NDS标准
    '''
    if dns_regex.match(value) == None:
        raise ValidationError('字符串不符合DNS标准')


def bucket_limit_validator(user):
    '''
    验证用户拥有的存储桶数量是否达到上限
    :param user: 无
    '''
    count = Bucket.get_user_valid_bucket_count(user=user)
    limit = BucketLimitConfig.get_user_bucket_limit(user=user)
    if count >= limit:
        raise ValidationError('您可以拥有的存储桶数量已达上限')


