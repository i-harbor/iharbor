from django import forms


class UploadFileForm(forms.Form):
    '''
    文件上传表单
    '''
    file = forms.FileField(label='上传文件', required=True,
                           widget=forms.FileInput(attrs={'class': 'btn btn-default form-control'}),)
