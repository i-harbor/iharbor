from django import forms

from .models import Bucket


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
    name = forms.CharField(label='存储桶名称', max_length=50,
                           widget=forms.TextInput(attrs={
                               'class': 'form-control',
                           }))

    def clean(self):
        # 检查存储桶是否已经存在
        bucket_name = self.cleaned_data['name']
        if Bucket.objects.filter(name=bucket_name).exists():
            raise forms.ValidationError('存储桶名已存在，请重新输入')


    def clean_name(self):
        bucket_name = self.cleaned_data['name']
        if not bucket_name:
            raise forms.ValidationError('存储桶bucket名称不能为空')
        return bucket_name