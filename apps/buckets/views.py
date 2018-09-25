from django.shortcuts import render, reverse, redirect
from django.http import Http404, JsonResponse, QueryDict
from django.contrib.auth.decorators import login_required
from django.views import View
from django.db.models import Q as dQ

from mongoengine.context_managers import switch_collection
from mongoengine.queryset.visitor import Q as mQ
from mongoengine.queryset import DoesNotExist, MultipleObjectsReturned

from .forms import UploadFileForm, BucketForm
from .models import BucketFileInfo, Bucket
from .utils import FileSystemHandlerBackend, get_collection_name

# Create your views here.

@login_required
def file_list(request, bucket_name=None, path=None):
    '''
    文件列表函数视图
    '''
    # bucket是否属于当前用户
    if not Bucket.objects.filter(dQ(user=request.user) & dQ(name=bucket_name)).exists():
        raise Http404('存储桶Bucket不存在')

    if request.method == 'POST':
        form = UploadFileForm(request.POST, request.FILES)
        if form.is_valid():
            file_handler = FileSystemHandlerBackend(request, bucket_name=bucket_name, cur_path=path,
                                                    action=FileSystemHandlerBackend.ACTION_STORAGE)
            file_handler.do_action()
            return redirect(reverse('buckets:file_list', kwargs={'bucket_name': bucket_name, 'path': path }))
    else:
        form = UploadFileForm()

    content = get_content(request, bucket_name=bucket_name, path=path)
    content['form'] = form
    return render(request, 'bucket.html', content)


def get_content(request, bucket_name, path):
    '''
    要返回的内容
    :return:
    '''
    content = {}
    content['submit_text'] = '上传'
    content['action_url'] = reverse('buckets:file_list', kwargs={
                                                            'bucket_name': bucket_name,
                                                            'path': path })
    content['bucket_name'] = bucket_name
    with switch_collection(BucketFileInfo, get_collection_name(username=request.user.username, bucket_name=bucket_name)):
        if path:
            # 获得当前目录节点id
            cur_dir_id = get_cur_dir_id(path)
            content['files'] = get_cur_dir_files(cur_dir_id)
        else:
            content['files'] = get_cur_dir_files(None)
    for f in content['files']:
        pass
    content['path_links'] = get_dir_link_paths(path)
    return content


def get_dir_link_paths(path):
    '''
    目录路径导航连接路径path
    :return: {dir_name: dir_full_path}
    '''
    dir_link_paths = {}
    if path == '':
        return dir_link_paths
    if path.endswith('/'):
        path = path.rstrip('/')
    dirs = path.split('/')
    for i, key in enumerate(dirs):
        dir_link_paths[key] = '/'.join(dirs[0:i+1])
    return dir_link_paths


def get_cur_dir_id(path):
    '''
    获得当前目录节点id
    @ return: 正常返回path目录的id；未找到记录返回None，即参数有误
    '''
    path = path.strip()
    if path.endswith('/'):
        path = path.rstrip('/')
    if not path:
        return None # path参数有误
    try:
        dir = BucketFileInfo.objects.get(mQ(na=path) & mQ(fod=False))  # 查找目录记录
    except DoesNotExist as e:
        pass
    except MultipleObjectsReturned as e:
        raise e

    return dir.id if dir else None  # None->未找到对应目录


def get_cur_dir_files(cur_dir_id):
    '''
    获得当前目录下的文件或文件夹记录

    :param cur_dir_id: 目录id;
    :return: 目录id下的文件或目录记录list; id==None时，返回存储桶下的文件或目录记录list
    '''
    if cur_dir_id:
        files = BucketFileInfo.objects(did=cur_dir_id).all()
    else:
        files = BucketFileInfo.objects(did__exists=False).all()  # did不存在表示是存储桶下顶层目录
    return files


@login_required
def download(request, id=None):
    '''
    下载文件函数视图
    '''
    if request.method == 'GET':
        #获取要下载的文件的uuid
        file_id = id
        bucket_name = request.GET.get('bucket_name', None)
        if not file_id or not bucket_name:
            raise Http404('要下载的文件不存在')

        file_handler = FileSystemHandlerBackend(request, id=file_id, bucket_name=bucket_name,
                                                action=FileSystemHandlerBackend.ACTION_DOWNLOAD)
        response = file_handler.do_action()
        if not response:
            raise Http404('要下载的文件不存在')
        return response


@login_required
def delete(request, id=None):
    '''
    删除文件函数视图
    '''
    if request.method == 'GET':
        #获取要下载的文件的uuid
        file_id = id
        bucket_name = request.GET.get('bucket_name', None)
        if not file_id or not bucket_name:
            raise Http404('要下载的文件不存在')
        path = request.GET.get('path', '')
        file_handler = FileSystemHandlerBackend(request, id=file_id, bucket_name=bucket_name,
                                                action=FileSystemHandlerBackend.ACTION_DELETE)
        if not file_handler.do_action():
            raise Http404('要删除的文件不存在')
        return redirect(to=request.META.get('HTTP_REFERER', reverse('buckets:file_list', kwargs={'bucket_name': bucket_name,
                                                                                                'path': path
                                                                                                })))



class BucketView(View):
    '''
    存储桶类视图
    '''
    def get(self, request):
        form = BucketForm()
        content = self.get_content(request=request, form=form)
        return render(request, 'buckets_home.html', context=content)


    def post(self, request):
        form = BucketForm(request.POST)
        #验证表单
        if form.is_valid():
            #创建存储桶bucket
            bucket_name = form.cleaned_data['name']
            user = request.user
            collection_name = get_collection_name(username=user.username, bucket_name=bucket_name)
            Bucket(name=bucket_name, user=user, collection_name=collection_name).save()
            # ajax请求
            if request.is_ajax():
                data = {
                    'code': 200,
                    'redirect_to': reverse('buckets:bucket_view') # 前端重定向地址
                }
                return JsonResponse(data=data)
            return redirect(to=reverse('buckets:bucket_view'))

        #表单验证有误
        if request.is_ajax(): # ajax请求
            data = {'code': 401, 'status': 'ERROR'}
            data['error_text'] = form.errors.as_text()
            return JsonResponse(data=data)
        content = self.get_content(request=request, form=form)
        return render(request, 'buckets_home.html', context=content)


    def delete(self, request):
        '''删除存储桶'''
        delete = QueryDict(request.body)
        ids = delete.getlist('ids')
        if ids:
            Bucket.objects.filter(id__in=ids).delete()
        data = {
            'code': 200,
            'code_text': '存储桶删除成功'
        }
        return JsonResponse(data=data)


    def get_content(self, request, form):
        content = {}
        content['submit_text'] = '创建'
        content['action_url'] = reverse('buckets:bucket_view')
        content['form'] = form
        content['buckets'] = Bucket.objects.filter(user=request.user).all()
        return content


