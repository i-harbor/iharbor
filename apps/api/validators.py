import re

from rest_framework.serializers import ValidationError


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

