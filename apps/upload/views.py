from django.shortcuts import render, reverse, redirect
from mongoengine.context_managers import switch_db

from .forms import UploadFileForm
from .models import UploadFileInfo
from .utils import FileSystemHandlerBackend

# Create your views here.


def file_list(request):
    '''
    文件列表函数视图
    '''
    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file_handler = FileSystemHandlerBackend(request, uuid=False,
                                                    action=FileSystemHandlerBackend.ACTION_STORAGE)
            file_handler.do_action()
            return redirect(reverse('upload:file_list'))
    else:
        form = UploadFileForm()

    content = {}
    content['submit_text'] = '上传'
    content['action_url'] = reverse('upload:file_list')
    content['form'] = form
    with switch_db(UploadFileInfo, 'db2'):
        content['files'] = UploadFileInfo.objects.all()
    return render(request, 'files.html', content)



def download(request, uuid=None):
    '''
    下载文件函数视图
    '''
    if request.method == 'GET':
        #获取要下载的文件的uuid
        file_uuid = uuid
        if file_uuid:
            file_handler = FileSystemHandlerBackend(request, uuid=file_uuid,
                                                    action=FileSystemHandlerBackend.ACTION_DOWNLOAD)
            response = file_handler.do_action()
            return response



def delete(request, uuid=None):
    '''
    删除文件函数视图
    '''
    if request.method == 'GET':
        #获取要下载的文件的uuid
        file_uuid = uuid
        if file_uuid:
            file_handler = FileSystemHandlerBackend(request, uuid=file_uuid,
                                                    action=FileSystemHandlerBackend.ACTION_DELETE)
            file_handler.do_action()
        return redirect(to=request.META.get('HTTP_REFERER', reverse('upload:file_list')))

