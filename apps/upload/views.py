from django.shortcuts import render, reverse, redirect
from django.http import Http404
from django.contrib.auth.decorators import login_required
from django.utils.decorators import method_decorator
from django.views import View
from django.db.models import Q

from mongoengine.context_managers import switch_collection, switch_db

from .forms import UploadFileForm, BucketForm
from .models import UploadFileInfo, Bucket
from .utils import FileSystemHandlerBackend, get_collection_name

# Create your views here.

@login_required
def file_list(request, bucket_name=None):
    '''
    文件列表函数视图
    '''
    # bucket是否属于当前用户
    if not Bucket.objects.filter(Q(user=request.user) & Q(name=bucket_name)).exists():
        raise Http404('存储桶Bucket不存在')

    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file_handler = FileSystemHandlerBackend(request, bucket_name=bucket_name, action=FileSystemHandlerBackend.ACTION_STORAGE)
            file_handler.do_action()
            return redirect(reverse('upload:file_list', kwargs={'bucket_name': bucket_name}))
    else:
        form = UploadFileForm()

    content = {}
    content['submit_text'] = '上传'
    content['action_url'] = reverse('upload:file_list', kwargs={'bucket_name': bucket_name})
    content['form'] = form
    content['bucket_name'] = bucket_name
    with switch_collection(UploadFileInfo, get_collection_name(username=request.user.username, bucket_name=bucket_name)):
        content['files'] = UploadFileInfo.objects.all()

    content['dir_links'] = {}
    return render(request, 'files.html', content)


@login_required
def download(request, uuid=None):
    '''
    下载文件函数视图
    '''
    if request.method == 'GET':
        #获取要下载的文件的uuid
        file_uuid = uuid
        bucket_name = request.GET.get('bucket_name', None)
        if not file_uuid or not bucket_name:
            raise Http404('要下载的文件不存在')

        file_handler = FileSystemHandlerBackend(request, uuid=file_uuid, bucket_name=bucket_name,
                                                action=FileSystemHandlerBackend.ACTION_DOWNLOAD)
        response = file_handler.do_action()
        if not response:
            raise Http404('要下载的文件不存在')
        return response


@login_required
def delete(request, uuid=None):
    '''
    删除文件函数视图
    '''
    if request.method == 'GET':
        #获取要下载的文件的uuid
        file_uuid = uuid
        bucket_name = request.GET.get('bucket_name', None)
        if not file_uuid or not bucket_name:
            raise Http404('要下载的文件不存在')

        file_handler = FileSystemHandlerBackend(request, uuid=file_uuid, bucket_name=bucket_name,
                                                action=FileSystemHandlerBackend.ACTION_DELETE)
        if not file_handler.do_action():
            raise Http404('要删除的文件不存在')
        return redirect(to=request.META.get('HTTP_REFERER', reverse('upload:file_list', kwargs={'bucket_name': bucket_name})))



class BucketView(View):
    '''
    存储桶类视图
    '''


    def get(self, request):
        form = BucketForm()
        content = self.get_content(request=request, form=form)
        return render(request, 'buckets.html', context=content)


    def post(self, request):
        form = BucketForm(request.POST)
        #验证表单
        if form.is_valid():
            #创建存储桶bucket
            bucket_name = form.cleaned_data['name']
            user = request.user
            collection_name = get_collection_name(username=user.username, bucket_name=bucket_name)
            Bucket(name=bucket_name, user=user, collection_name=collection_name).save()
            return redirect(to=reverse('upload:bucket_view'))

        #表单验证有误
        content = self.get_content(request=request, form=form)
        return render(request, 'buckets.html', context=content)


    def delete(self, request):
        pass


    def get_content(self, request, form):
        content = {}
        content['submit_text'] = '创建'
        content['action_url'] = reverse('upload:bucket_view')
        content['form'] = form
        content['buckets'] = Bucket.objects.filter(user=request.user).all()
        return content


