import hmac
import time
from hashlib import sha1
import json
from base64 import urlsafe_b64encode, urlsafe_b64decode
import requests

def b(data):
    '''
    str to bytes by utf-8
    '''
    if isinstance(data, str):
        return data.encode('utf-8')
    return data


def s(data):
    '''
    bytes to str by utf-8
    '''
    if isinstance(data, bytes):
        data = data.decode('utf-8')
    return data


def urlsafe_base64_encode(data):
    '''
    对提供的数据进行urlsafe的base64编码

    :param data: 待编码的数据，一般为字符串
    :return: 编码后的字符串
    '''
    ret = urlsafe_b64encode(b(data))
    return s(ret)


def urlsafe_base64_decode(data):
    '''
    对提供的urlsafe的base64编码的数据进行解码

    :param data: 待解码的数据，一般为字符串
    :return: 解码后的字符串
    '''
    ret = urlsafe_b64decode(s(data))
    return ret

def generate_token(data_b64, secret_key):
    '''
    通过密钥加密编码生成一个token

    :param data_b64: 待编码的内容，一般为字符串
    :param secret_key: 密钥
    :return: 解码后的token字符串
    '''
    data = b(data_b64)
    hashed = hmac.new(secret_key, data, sha1)
    return urlsafe_base64_encode(hashed.digest())

class AuthKey(object):
    '''
    该类主要用于evharbor安全凭证的生成
    '''
    def __init__(self, access_key, secret_key):
        self.__check_key(access_key, secret_key)
        self.__access_key = access_key
        self.__secret_key = b(secret_key)

    @staticmethod
    def __check_key(access_key, secret_key):
        if not (access_key and secret_key):
            raise ValueError('invalid key')

    def get_access_key(self):
        return self.__access_key

    def auth_key(self, path_of_url, data=None, timedelta=None):
        '''
        生成安全凭证
        :param path_of_url: url的全路径, 如url为http://abc.com/api/?a=b时，path_of_url为 /api/?a=b
        :param body: 请求时提交的数据（如果需要），类型dict，只包含普通数据，不包含文件等流数据
        :param timedelta: 安全凭证的有效期时间增量（基于当前时间戳）
        :return:
            {access_key}:{token}:{data_b64}
        '''
        if isinstance(timedelta, int):
            deadline = self.get_deadline(timedelta=timedelta)
        else:
            deadline = self.get_deadline()

        body = dict(path_of_url=path_of_url, deadline=deadline)
        if data is not None and isinstance(data, dict):
            body.update(data)

        data_json = json.dumps(body, separators=(',', ':'))
        data_b64 = urlsafe_base64_encode(data_json)
        return '{0}:{1}:{2}'.format(self.__access_key, generate_token(data_b64, self.__secret_key), data_b64)

    def auth_header_value(self, auth_key):
        '''
        HTTP标头Authorization的值
        :param auth_key:
        :return:
        '''
        return '{0} {1}'.format('evhb-auth', auth_key)

    def get_deadline(self, timedelta=3600):
        '''
        获取过期时间戳
        :param timedelta: 时间增量, 默认3600s
        :return: 时间戳timestamp
        '''
        deadline = int(time.time()) + timedelta
        return deadline







