from django.shortcuts import render, redirect, reverse
from django.contrib.auth.views import redirect_to_login
from django.contrib.auth import logout, login
from django.contrib.auth.decorators import login_required
from django.contrib.auth import get_user_model
from django.template.loader import get_template
from rest_framework.authtoken.models import Token

from .forms import UserRegisterForm, LoginForm, PasswordChangeForm
from .models import Email

#获取用户模型
User = get_user_model()

# Create your views here.

def register_user(request):
    '''
    用户注册函数视图
    '''
    if request.method == 'POST':
        form = UserRegisterForm(request.POST)
        #表单数据验证通过
        if form.is_valid():
            cleaned_data = form.cleaned_data
            username = cleaned_data.get('username', '')
            email = username
            password = cleaned_data.get('password', '')

            # 创建非激活状态新用户并保存
            user = User.objects.create_user(username=username, password=password, email=email, is_active=False)

            logout(request)#登出用户（确保当前没有用户登陆）

            # 向邮箱发送激活连接
            if send_active_url_email(request, email, user):
                return render(request, 'message.html', context={'message': '用户注册成功，请登录邮箱访问收到的连接以激活用户'})

            form.add_error(None, '邮件发送失败，请检查邮箱输入是否有误')
    else:
        form = UserRegisterForm()

    content = {}
    content['form_title'] = '用户注册'
    content['submit_text'] = '注册'
    content['action_url'] = reverse('users:register')
    content['form'] = form
    return render(request, 'form.html', content)


def login_user(request):
    '''
    用户登陆函数视图
    '''
    if request.method == 'POST':
        form = LoginForm(request.POST)
        if form.is_valid():
            user = form.cleaned_data['user']
            #登陆用户
            login(request, user)
            next = request.session.get('next', reverse('buckets:bucket_view'))
            return redirect(to=next)
    else:
        #保存登陆后跳转地址，如果存在的话
        next = request.GET.get('next', None)
        if next:
            request.session['next'] = next
        form = LoginForm()

    content = {}
    content['form_title'] = '用户登录'
    content['submit_text'] = '登录'
    content['action_url'] = reverse('users:login')
    content['form'] = form
    return render(request, 'form.html', content)


@login_required
def logout_user(request):
    '''
    注销用户
    '''
    logout(request)
    return redirect(to=request.GET.get('next', reverse('buckets:bucket_view')))


@login_required
def change_password(request):
    '''
    修改密码函数视图
    '''
    if request.method == 'POST':
        form = PasswordChangeForm(request.POST, user=request.user)
        if form.is_valid():
            #修改密码
            new_password = form.cleaned_data['new_password']
            user = request.user
            user.set_password(new_password)
            ret = user.save()

            #注销当前用户，重新登陆
            logout(request)
            return redirect(to=reverse('users:login'))
    else:
        form = PasswordChangeForm()

    content = {}
    content['form_title'] = '修改密码'
    content['submit_text'] = '修改'
    content['action_url'] = reverse('users:change_password')
    content['form'] = form
    return render(request, 'form.html', content)


def active_user(request):
    '''
    激活用户
    :param request:
    :return:
    '''
    urls = []
    try:
        urls.append({'url': reverse('users:login'), 'name': '登录'})
        urls.append({'url': reverse('users:register'), 'name': '注册'})
    except:
        pass

    key = request.GET.get('token', None)
    try:
        token = Token.objects.select_related('user').get(key=key)
    except Token.DoesNotExist:
        return render(request, 'message.html', context={'message': '用户激活失败，无待激活账户，或者账户已被激活，请直接尝试登录', 'urls': urls})

    user = token.user
    user.is_active = True
    user.save()

    reflesh_new_token(token)

    return render(request, 'message.html', context={'message': '用户已激活', 'urls': urls})


def get_active_link(request, user):
    '''
    获取账户激活连接

    :param request: 请求对象
    :param user: 用户对象
    :return: 正常: url
            错误：None
    '''
    token = get_or_create_token(user=user)
    if not token:
        return None

    try:
        active_link = reverse('users:active')
    except:
        return None

    active_link = request.build_absolute_uri(active_link)
    active_link += f'?token={token.key}'
    return active_link


def send_active_url_email(request, to_email, user):
    '''
    发送用户激活连接邮件
    
    :param email: 邮箱
    :param user: 用户对象
    :return: True(发送成功)，False(发送失败)
    '''
    active_link = get_active_link(request, user)
    if not active_link:
        return False

    message = f'''
        亲爱的用户：
            欢迎使用EVHarbor,您已使用本邮箱成功注册账号，请访问下面激活连接以激活账户,如非本人操作请忽略此邮件。
            激活连接：{active_link}
        '''

    email = Email()
    email.message = active_link
    ok = email.send_active_email(receiver=to_email, message=message)
    if ok:
        return True
    return False



def get_or_create_token(user):
    token, created = Token.objects.get_or_create(user=user)
    if not token:
        return None

    return token

def reflesh_new_token(token):
    token.delete()
    token.key = token.generate_key()
    token.save()


