from django.shortcuts import render, redirect, reverse
from django.contrib.auth.views import redirect_to_login
from django.contrib.auth import logout, login
from django.contrib.auth.decorators import login_required

from .forms import UserRegisterForm, LoginForm, PasswordChangeForm

# Create your views here.

def register_user(request):
    '''
    用户注册函数视图
    '''
    if request.method == 'POST':
        form = UserRegisterForm(request.POST)
        #表单数据验证通过
        if form.is_valid():
            #保存用户
            user = form.cleaned_data['user']
            user.save()

            #从定向到登陆界面，登陆通过后导航到next地址
            logout(request)#登出用户（确保当前没有用户登陆）
            return redirect_to_login(next=reverse('bucket_view'), login_url=reverse('users:login'))
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
            next = request.session.get('next', reverse('upload:bucket_view'))
            return redirect(to=next)
    else:
        #保存登陆后跳转地址，如果存在的话
        next = request.GET.get('next', None)
        if next:
            request.session['next'] = next
        form = LoginForm()

    content = {}
    content['form_title'] = '用户登陆'
    content['submit_text'] = '登陆'
    content['action_url'] = reverse('users:login')
    content['form'] = form
    return render(request, 'form.html', content)


@login_required
def logout_user(request):
    '''
    注销用户
    '''
    logout(request)
    return redirect(to=request.GET.get('next', reverse('upload:bucket_view')))


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


