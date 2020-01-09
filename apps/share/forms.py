from django import forms


class SharePasswordForm(forms.Form):
    '''
    分享密码表单
    '''
    password = forms.CharField( label='分享密码',
                                min_length=4,
                                max_length=10,
                                widget=forms.TextInput(attrs={
                                                'class': 'form-control',
                                                'placeholder': '请输入分享密码'
                                }))

    # def clean(self):
    #     '''
    #     在调用is_valid()后会被调用
    #     '''
    #     password = self.cleaned_data.get('password')
    #     return self.cleaned_data
