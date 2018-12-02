from django.shortcuts import render, redirect, reverse
from django.contrib.auth.views import redirect_to_login
from django.contrib.auth import logout, login
from django.contrib.auth.decorators import login_required
from django.contrib.auth import get_user_model
from rest_framework.authtoken.models import Token

from .forms import UserRegisterForm, LoginForm, PasswordChangeForm

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
            email = cleaned_data.get('email', '')
            password = cleaned_data.get('password', '')

            # 创建非激活状态新用户并保存
            User.objects.create_user(username=username, password=password, email=email, is_active=False)

            #从定向到登陆界面，登陆通过后导航到next地址
            logout(request)#登出用户（确保当前没有用户登陆）

            # 向邮箱发送激活连接

            # return redirect_to_login(next=reverse('buckets:bucket_view'), login_url=reverse('users:login'))
            return render(request, 'message.html', context={'message': '用户注册成功，请登录邮箱访问收到的连接以激活用户'})
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
    key = request.GET.get('token', None)
    try:
        token = Token.objects.select_related('user').get(key=key)
    except Token.DoesNotExist:
        return render(request, 'message.html', context={'message': '用户激活失败，token不存在'})

    user = token.user
    user.is_active = True
    user.save()

    return render(request, 'message.html', context={'message': '用户已激活', 'login': reverse('users:login')})


def send_active_url_email(email, user):
    '''
    发送用户激活连接邮件
    
    :param email: 邮箱
    :param user: 用户对象
    :return: True(发送成功)，False(发送失败)
    '''
    pass



