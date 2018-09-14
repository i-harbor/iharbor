from django import forms
from django.contrib.auth import authenticate

from .models import UserProfile


class UserRegisterForm(forms.Form):
    '''
    用户注册表单
    '''
    username = forms.CharField( label='用户名',
                                min_length=6,
                                max_length=20,
                                widget=forms.TextInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': '请输入6-20个数字和字母组合的用户名'
                                }))
    password = forms.CharField( label='密码',
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': '请输入一个8-20位的密码'
                                }))
    confirm_password = forms.CharField( label='确认密码',
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': '请输入确认密码'
                                }))

    def clean(self):
        '''
        验证表单提交的数据
        '''
        # username = getattr(self.cleaned_data, 'username', None)
        username = self.cleaned_data['username']
        password = self.cleaned_data['password']
        confirm_password = self.cleaned_data['confirm_password']

        #用户名输入是否为空
        if not username:
            raise forms.ValidationError('用户名不能为空')

        #检查用户名是否已存在
        if UserProfile.objects.filter(username=username).exists():
            raise forms.ValidationError('用户名已存在，请重新输入')

        #密码是否一致
        if not password or password != confirm_password:
            raise forms.ValidationError('密码输入不一致')

        #创建新用户
        user = UserProfile.objects.create_user(username=username, password=password)
        self.cleaned_data['user'] = user

        return self.cleaned_data



class LoginForm(forms.Form):
    '''
    用户登陆表单
    '''
    username = forms.CharField( label='用户名',
                                min_length=6,
                                max_length=20,
                                widget=forms.TextInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': '请输入6-20个数字和字母组合的用户名'
                                }))
    password = forms.CharField( label='密码',
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': '请输入一个8-20位的密码'
                                }))


    def clean(self):
        '''
        验证表单提交的数据
        '''
        username = self.cleaned_data['username']
        password = self.cleaned_data['password']

        #验证用户
        user = authenticate(username=username, password=password)
        if not user:
            raise forms.ValidationError('用户名或密码有误，请注意区分字母大小写')
        else:
            self.cleaned_data['user'] = user
        return self.cleaned_data



class PasswordChangeForm(forms.Form):
    '''
    用户密码修改表单
    '''
    old_password = forms.CharField( label='原密码',
                                min_length=8,
                                max_length=20,
                                widget=forms.TextInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': '请输入原密码'
                                }))
    new_password = forms.CharField( label='新密码',
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': '请输入一个8-20位的新密码'
                                }))
    confirm_new_password = forms.CharField( label='确认新密码',
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': '请再次输入新密码'
                                }))

    def __init__(self, *args, **kwargs):
        if 'user' in kwargs:
            self.user = kwargs.pop('user')
        return super(PasswordChangeForm, self).__init__(*args, **kwargs)

    def clean(self):
        '''
        验证表单提交的数据
        '''
        new_password = self.cleaned_data['new_password']
        confirm_new_password = self.cleaned_data['confirm_new_password']
        if new_password != confirm_new_password or not new_password:
            raise forms.ValidationError('新密码输入不一致，请重新输入')
        return self.cleaned_data


    def clean_old_password(self):
        '''
        验证原密码
        '''
        old_password = self.cleaned_data['old_password']
        if not self.user.check_password(old_password):
            raise forms.ValidationError('原密码有误')
        return old_password

