from django import forms
from django.contrib.auth import authenticate
from django.contrib.auth import get_user_model
from django.core.exceptions import ObjectDoesNotExist
from django.utils.translation import gettext_lazy
from django.utils.translation import gettext as _

from utils.jwt_token import JWTokenTool2

#获取用户模型
User = get_user_model()


class UserRegisterForm(forms.Form):
    '''
    用户注册表单
    '''
    username = forms.EmailField(label=gettext_lazy('用户名(邮箱)'),
                                required=True,
                                max_length=100,
                                widget=forms.EmailInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': gettext_lazy('请输入邮箱作为用户名')
                                }))
    password = forms.CharField( label=gettext_lazy('密码'),
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': gettext_lazy('请输入一个8-20位的密码')
                                }))
    confirm_password = forms.CharField( label=gettext_lazy('确认密码'),
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': gettext_lazy('请输入确认密码')
                                }))

    last_name = forms.CharField(label=gettext_lazy('姓氏'), max_length=30,
                                widget=forms.TextInput(attrs={
                                    'class': 'form-control',
                                    'placeholder': gettext_lazy('请如实填写')
                                }))

    first_name = forms.CharField(label=gettext_lazy('名字'), max_length=30,
                                 widget=forms.TextInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': gettext_lazy('请如实填写')
                                            }))

    telephone = forms.CharField(label=gettext_lazy('电话'), max_length=11,
                                widget=forms.TextInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': gettext_lazy('请如实填写')
                                            }))
    company = forms.CharField(label=gettext_lazy('公司/单位'), max_length=255,
                              widget=forms.TextInput(attrs={
                                                  'class': 'form-control',
                                                  'placeholder': gettext_lazy('请如实填写')
                                                }))

    def clean(self):
        '''
        验证表单提交的数据
        '''
        username = self.cleaned_data.get('username', '')
        password = self.cleaned_data.get('password', '')
        confirm_password = self.cleaned_data.get('confirm_password', '')

        #用户名输入是否为空
        if not username:
            if not self.has_error('username'):
                raise forms.ValidationError(_('用户名不能为空'))

        #检查用户名是否已存在
        user = User.objects.filter(username=username).first()
        if user:
            if user.is_active:
                raise forms.ValidationError(_('用户名已存在，请重新输入'))
            else:
                self.cleaned_data['user'] = user # 未激活用户

        #密码是否一致
        if not password or password != confirm_password:
            raise forms.ValidationError(_('密码输入不一致'))

        return self.cleaned_data

    def get_or_creat_unactivated_user(self):
        '''
        获取或者创建未激活用户

        :return: 正常-> user; 错误-> None
        '''
        cleaned_data = self.cleaned_data
        user = cleaned_data.get('user', None)
        email = username = cleaned_data.get('username', '')
        password = cleaned_data.get('password', '')
        first_name = cleaned_data.get('first_name')
        last_name = cleaned_data.get('last_name')
        telephone = cleaned_data.get('telephone')
        company = cleaned_data.get('company')

        # 如果不是已注册未激活用户
        if not user:
            # 创建非激活状态新用户
            user = User(username=username, is_active=False)

        user.email = email
        user.set_password(password)
        user.first_name = first_name
        user.last_name = last_name
        user.telephone = telephone
        user.company = company
        try:
            user.save()
        except:
            return None
        return user


class LoginForm(forms.Form):
    '''
    用户登陆表单
    '''
    username = forms.CharField( label=gettext_lazy('用户名(邮箱)'),
                                required=True,
                                max_length=100,
                                widget=forms.TextInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': gettext_lazy('请输入用户名')
                                }))
    password = forms.CharField( label=gettext_lazy('密码'),
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': gettext_lazy('请输入一个8-20位的密码')
                                }))

    def clean(self):
        '''
        验证表单提交的数据
        '''
        username = self.cleaned_data.get('username')
        password = self.cleaned_data.get('password')

        #验证用户
        try:
            user = User.objects.get_by_natural_key(username)
        except User.DoesNotExist:
            raise forms.ValidationError(_('用户名或密码有误，请注意区分字母大小写'))

        if not user.is_active:
            raise forms.ValidationError(_('用户未激活'))
        
        if not user.check_password(password):
            raise forms.ValidationError(_('用户名或密码有误，请注意区分字母大小写'))
        else:
            if user.third_app != user.LOCAL_USER:
                user.third_app = user.LOCAL_USER # 本地用户登录
                user.save()
            self.cleaned_data['user'] = user
        return self.cleaned_data



class PasswordChangeForm(forms.Form):
    '''
    用户密码修改表单
    '''
    old_password = forms.CharField( label=gettext_lazy('原密码'),
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': gettext_lazy('请输入原密码')
                                }))
    new_password = forms.CharField( label=gettext_lazy('新密码'),
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': gettext_lazy('请输入一个8-20位的新密码')
                                }))
    confirm_new_password = forms.CharField( label=gettext_lazy('确认新密码'),
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': gettext_lazy('请再次输入新密码')
                                }))

    def __init__(self, *args, **kwargs):
        if 'user' in kwargs:
            self.user = kwargs.pop('user')
        super(PasswordChangeForm, self).__init__(*args, **kwargs)

    def clean(self):
        '''
        验证表单提交的数据
        '''
        new_password = self.cleaned_data.get('new_password')
        confirm_new_password = self.cleaned_data.get('confirm_new_password')
        if new_password != confirm_new_password or not new_password:
            raise forms.ValidationError(_('密码输入不一致'))
        return self.cleaned_data


    def clean_old_password(self):
        '''
        验证原密码
        '''
        old_password = self.cleaned_data.get('old_password')

        # 如果当前用户为第三方登录，且还未设置本地密码，跳过原密码检验
        if self.user.third_app != self.user.LOCAL_USER and not self.user.password:
            return old_password

        if not self.user.check_password(old_password):
            raise forms.ValidationError(_('原密码有误'))
        return old_password


class ForgetPasswordForm(forms.Form):
    '''
    忘记密码用户名表单
    '''
    username = forms.EmailField(label=gettext_lazy('用户名(邮箱)'),
                               max_length=100,
                               widget=forms.EmailInput(attrs={
                                   'class': 'form-control',
                                   'placeholder': gettext_lazy('请输入用户名')}))

    def clean(self):
        '''
        在调用is_valid()后会被调用
        '''
        username = self.cleaned_data.get('username', '')

        #用户名输入是否为空
        if not username:
            if not self.has_error('username'):
                raise forms.ValidationError(_('用户名不能为空'))
        if username:
            try:
                user = User.objects.get(username=username)
                self.cleaned_data['user'] = user
            except ObjectDoesNotExist:
                raise forms.ValidationError(_('用户不存在'))

        return self.cleaned_data


class PasswordResetForm(forms.Form):
    '''
    密码用户名表单
    '''
    jwt = forms.CharField( label=None, max_length=1000,
                           widget=forms.HiddenInput(attrs={
                               'class': 'form-control',
                               'placeholder': 'jwt-value',}))

    new_password = forms.CharField( label=gettext_lazy('新密码'),
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': gettext_lazy('请输入一个8-20位的新密码')
                                }))
    confirm_new_password = forms.CharField( label=gettext_lazy('确认新密码'),
                                min_length=8,
                                max_length=20,
                                widget=forms.PasswordInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': gettext_lazy('请再次输入新密码')
                                }))

    def clean(self):
        '''
        在调用is_valid()后会被调用
        '''
        jwt = self.cleaned_data.get('jwt', '')
        new_password = self.cleaned_data.get('new_password')
        confirm_new_password = self.cleaned_data.get('confirm_new_password')

        if new_password != confirm_new_password or not new_password:
            raise forms.ValidationError(_('密码输入不一致'))

        jwtt = JWTokenTool2()
        try:
            user = jwtt.verify_jwt_return_user(jwt)
        except:
            user = None

        if not user:
            raise forms.ValidationError(_('重置密码失败，jwt无效或已过期，请重新找回密码获取新的链接'))

        self.cleaned_data['user'] = user

        return self.cleaned_data


