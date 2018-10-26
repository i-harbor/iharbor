from django import forms

from .models import Bucket
from .validators import DNSStringValidator

class UploadFileForm(forms.Form):
    '''
    文件上传表单
    '''
    file = forms.FileField(label='上传文件', required=True,
                           widget=forms.FileInput(attrs={'class': 'btn btn-default form-control'}),)


class BucketForm(forms.Form):
    '''
    创建存储桶表单
    '''
    name = forms.CharField(label='存储桶名称', required=False, max_length=63,
                           help_text='请输入符合DNS标准的存储桶名称，英文字母、数字和-组成，不超过63个字符',
                           widget=forms.TextInput(attrs={
                               'class': 'form-control',
                           }))

    def clean(self):
        # 检查存储桶是否已经存在
        bucket_name = self.cleaned_data['name']

        if not bucket_name:
            raise forms.ValidationError('存储桶bucket名称不能为空')

        if bucket_name.startswith('-') or bucket_name.endswith('-'):
            raise forms.ValidationError('存储桶bucket名称不能以“-”开头或结尾')

        DNSStringValidator(bucket_name)
        self.cleaned_data['name'] = bucket_name.lower() #

        if Bucket.objects.filter(name=bucket_name).exists():
            raise forms.ValidationError('存储桶名已存在，请重新输入')




