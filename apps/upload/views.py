import os
import uuid

from django.shortcuts import render, reverse, redirect
from django.conf import settings
from mongoengine.context_managers import switch_db

from .forms import UploadFileForm
from .models import UploadFileInfo

# Create your views here.


def file_list(request):
    '''
    文件列表函数视图
    '''
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            handleUploadFile(request)
            return redirect(reverse('upload:file_list'))
    else:
        form = UploadFileForm()

    content = {}
    # content['form_title'] = '上传文件'
    content['submit_text'] = '上传'
    content['action_url'] = reverse('upload:file_list')
    content['form'] = form
    with switch_db(UploadFileInfo, 'db2'):
        content['files'] = UploadFileInfo.objects.all()
    return render(request, 'files.html', content)


def handleUploadFile(request):
    '''
    保存用户上传的文件，并创建文件信息记录
    '''
    obj = request.FILES.get('file')
    #保存文件的路径
    dir_url = os.path.join(settings.MEDIA_ROOT, 'upload')
 
    #路径不存在时创建路径
    if not os.path.exists(dir_url):
        os.makedirs(dir_url)  

    #保存上传的文件，文件名为uuid
    uid = uuid.uuid1()
    full_path_filename = os.path.join(dir_url, str(uid))
    with open(full_path_filename, 'wb') as f:
        for chunk in obj.chunks():
            f.write(chunk)

    #保存对应文件记录到指定数据库
    with switch_db(UploadFileInfo, 'db2'):
        UploadFileInfo(uuid=uid.hex, filename=obj.name, size=obj.size).save()



