from django.shortcuts import redirect
from django.conf import settings
from django.contrib.auth import get_user_model, login
import requests
import json

#获取用户模型
User = get_user_model()

def kjy_login_callback(request, *args, **kwargs):
    '''
    第三方科技云通行证登录回调视图
    :return:
    '''
    code = request.GET.get('code', None)
    if not code:
        return kjy_logout()

    token = get_kjy_auth_token(code=code)
    if not token:
        return kjy_logout()

    user =  create_kjy_auth_user(request, token)
    if not user:
        return kjy_logout()

    # 标记当前为科技云通行证登录用户
    if user.third_app != user.THIRD_APP_KJY:
        user.third_app = user.THIRD_APP_KJY
        user.save()
    # 登录用户
    login(request, user)
    return redirect(to='/')


def create_kjy_auth_user(request, token):
    '''
    创建科技云通行证登录认证的用户

    :param token:  科技云通行证登录认证token
    :return:
        success: User()
        failed: None
    '''
    user_info = get_user_info_from_token(token)
    if not user_info:
        return None

    is_active = user_info.get('cstnetIdStatus')
    email = user_info.get('cstnetId')
    truename = user_info.get('truename')

    if is_active != 'active': # 未激活用户
        return None

    # 邮箱对应用户是否已存在
    try:
        user = User.objects.filter(username=email).first()
    except Exception as e:
        user = None
    # 已存在, 返回用户
    if user:
        return user

    # 创建用户
    try:
        first_name, last_name = get_first_and_last_name(truename)
    except Exception:
        first_name, last_name = '', ''
    user = User(username=email, email=email, first_name=first_name, last_name=last_name)
    try:
        user.save()
    except Exception as e:
        return None

    return user


def get_first_and_last_name(name:str):
    '''
    粗略切分姓和名
    :param name: 姓名
    :return:
        (first_name, last_name)
    '''
    if not name:
        return ('', '')

    # 如果是英文名
    if name.replace(' ','').encode('UTF-8').isalpha():
        names = name.rsplit(' ', maxsplit=1)
        if len(names) == 2:
            first_name, last_name = names
        else:
            first_name, last_name = name, ''
    elif len(name) == 4:
        first_name, last_name = name[2:], name[:2]
    else:
        first_name, last_name = name[1:], name[:1]

    # 姓和名长度最大为30
    if len(first_name) > 30:
        first_name = first_name[:31]

    if len(last_name) > 30:
        last_name = last_name[:31]

    return (first_name, last_name)

def get_kjy_login_url():
    '''
    获取 中国科技云通行证登录url
    :return:
        success: url
        failed: None
    '''
    kjy_settings = settings.THIRD_PARTY_APP_AUTH.get('SCIENCE_CLOUD')
    kjy_security_settings = settings.THIRD_PARTY_APP_AUTH_SECURITY.get('SCIENCE_CLOUD')
    if not kjy_settings or not kjy_security_settings:
        return None

    client_id = kjy_security_settings.get('client_id')
    client_callback_url = kjy_settings.get('client_callback_url')
    login_url = kjy_settings.get('login_url')
    params = {
        'client_id': client_id,
        'redirect_uri': client_callback_url,
    }
    try:
        url = prepare_url(url=login_url, params=params)
    except:
        return None

    return url

def get_kjy_auth_token(code):
    '''
    获取登录认证后的token

    :param code: 认证成功后回调url中的param参数code
    :return:
        success:
        {
            "access_token":  "SlAV32hkKG",
            "expires_in":  3600,
            “refresh_token:  ”ASAEDFIkie876”,
            ”userInfo”: {
                “umtId”:  12,                     # 对应umt里面的id号
                “truename”:  ”yourName”,        # 用户真实姓名
                ”type”:  ”umtauth”,             # 账户所属范围umt、coremail、uc
                ”securityEmail”: ” securityEmail”, # 密保邮箱
                ”cstnetIdStatus”: ”cstnetIdStatus”, # 主账户激活状态，即邮箱验证状态， 可选值：active-已激活，temp-未激活。应用可根据此状态判断是否允许该用户登录
                ”cstnetId”: ”yourEmail”,            # 用户主邮箱
                “passwordType”:” password_umt”,     # 登录的密码类型
                ”secondaryEmails”:[“youremail1”, “youremail2”] # 辅助邮箱
            }
        }

        failed: None
    '''
    kjy_settings = settings.THIRD_PARTY_APP_AUTH.get('SCIENCE_CLOUD')
    kjy_security_settings = settings.THIRD_PARTY_APP_AUTH_SECURITY.get('SCIENCE_CLOUD')
    if not kjy_settings or not kjy_security_settings:
        return None

    client_id = kjy_security_settings.get('client_id')
    client_secret = kjy_security_settings.get('client_secret')
    client_callback_url = kjy_settings.get('client_callback_url')
    token_url = kjy_settings.get('token_url')
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': client_callback_url,
        'code': code,
        'grant_type': 'authorization_code'
    }

    try:
        r = requests.post(url=token_url, data=data)
        if r.status_code == 200:
            token = r.json()
        else:
            token = None
    except Exception as e:
        return None

    return token

def get_user_info_from_token(token):
    '''
    从token中获取用户信息

    :param token:
    :return:
        success: user_info ; type:dict
        failed: None
    '''
    user_info = token.get('userInfo')
    if isinstance(user_info, str):
        try:
            user_info = json.loads(user_info)
        except:
            user_info = None

    return user_info


def get_kjy_logout_url(redirect_uri=None):
    '''
    科技云通行证登出url

    :param redirect_uri: 登出后重定向的url
    :return:
        success: url
        failed: None
    '''
    kjy_settings = settings.THIRD_PARTY_APP_AUTH.get('SCIENCE_CLOUD')
    if not kjy_settings:
        return None

    logout_url = kjy_settings.get('logout_url')
    if not redirect_uri:
        redirect_uri = kjy_settings.get('client_home_url')

    try:
        url = prepare_url(url=logout_url, params={'WebServerURL': redirect_uri})
    except:
        return None

    return url

def kjy_logout(next=None):
    '''
    登出科技云账户

    :param next: 登出后重定向的url
    :return:
    '''
    url = get_kjy_logout_url(next)
    return redirect(to=url)

def prepare_url(url, params=None):
    '''
    拼接url

    :param url: url
    :param params: 参数，type:dict
    :return:
    '''
    pr = requests.PreparedRequest()
    pr.prepare_url(url=url, params=params)
    # url = requests.utils.unquote(pr.url)  # 解码url
    return pr.url
