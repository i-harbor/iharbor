from django.shortcuts import render, reverse, redirect
from django.http import Http404, JsonResponse, QueryDict, FileResponse
from django.contrib.auth.decorators import login_required
from django.views import View
from django.db.models import Q as dQ

from mongoengine.context_managers import switch_collection
from mongoengine.queryset.visitor import Q as mQ
from mongoengine.queryset import DoesNotExist, MultipleObjectsReturned

from .forms import UploadFileForm, BucketForm
from .models import BucketFileInfo, Bucket
from .utils import get_collection_name, BucketFileManagement
from utils.storagers import FileStorage

# Create your views here.


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
            collection_name = get_collection_name(bucket_name=bucket_name)
            Bucket(name=bucket_name, user=user, collection_name=collection_name).save()
            # ajax请求
            if request.is_ajax():
                data = {
                    'code': 200,
                    'code_text': '创建存储桶“{0}”成功'.format(bucket_name),
                    'bucket_name': bucket_name,
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
            buckets = Bucket.objects.filter(id__in=ids)
            for bucket in buckets:
                # with switch_collection(BucketFileInfo, get_collection_name(bucket_name=bucket.name)):
                #     BucketFileInfo.drop_collection()
                bucket.do_soft_delete() # 软删除

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
        content['buckets'] = Bucket.objects.filter(dQ(user=request.user) & dQ(soft_delete=False)).all()
        return content


class FileView(View):
    '''
    存储桶文件类视图
    '''
    def get(self, request, *args, **kwargs):
        '''
        文件列表函数视图
        '''
        bucket_name = kwargs.get('bucket_name')
        path = kwargs.get('path')

        # bucket是否属于当前用户
        self.check_user_own_bucket(request, bucket_name)

        content = self.get_content(request, bucket_name=bucket_name, path=path)
        content['form'] = UploadFileForm()
        return render(request, 'bucket.html', content)


    def get_content(self, request, bucket_name, path):
        '''
        要返回的内容
        :return:
        '''
        content = {}
        content['submit_text'] = '上传'
        content['action_url'] = reverse('buckets:file_list', kwargs={
            'bucket_name': bucket_name,
            'path': path})
        content['ajax_upload_url'] = reverse('api:upload-list', kwargs={'version': 'v1'})
        content['bucket_name'] = bucket_name
        bfm = BucketFileManagement(path=path)
        with switch_collection(BucketFileInfo,
                               get_collection_name(bucket_name=bucket_name)):
            ok, files = bfm.get_cur_dir_files()
            if ok:
                content['files'] = files
            else:
                raise Http404('参数有误，未找到相关记录')

        content['path_links'] = bfm.get_dir_link_paths()
        content['cur_path'] = path
        return content

    def check_user_own_bucket(self, request, bucket_name):
        # bucket是否属于当前用户
        if not Bucket.objects.filter(dQ(user=request.user) & dQ(name=bucket_name)).exists():
            raise Http404('您不存在一个存储桶'+ bucket_name)


class FileObjectView(View):
    '''
    文件对象信息类视图
    '''
    def get(self, request, *args, **kwargs):
        '''文件对象信息'''
        bucket_name = kwargs.get('bucket_name')
        path = kwargs.get('path')
        object_name = kwargs.get('object_name')

        bfm = BucketFileManagement(path=path)
        content = {}
        file = self.get_file_obj(bfm, bucket_name, object_name)
        if not file:
            raise Http404('文件不存在')
        content['file'] = file
        content['bucket_name'] = bucket_name
        content['path_links'] = bfm.get_dir_link_paths()
        content['object_link'] = request.build_absolute_uri(reverse('buckets:get_object_view',
                                         kwargs={'bucket_name': bucket_name, 'path': path, 'object_name': object_name}))
        return render(request, 'fileobject.html', content)

    def delete(self, request, *args, **kwargs):
        '''删除文件对象'''
        bucket_name = kwargs.get('bucket_name')
        path = kwargs.get('path')
        object_name = kwargs.get('object_name')

        bfm = BucketFileManagement(path=path)
        file = self.get_file_obj(bfm, bucket_name, object_name)
        if file:
            # do rados delete object
            file.switch_collection(get_collection_name(bucket_name=bucket_name))
            file.delete()
            data = {
                'code': 200,
                'code_text': '文件删除成功'
            }
        else:
            data = {
                'code': 404,
                'code_text': '文件不存在'
            }
        return JsonResponse(data=data)

    def get_file_obj(self, bfm, bucket_name, object_name):
        '''
        获取文件对象
        :param bucket_name: 存储桶名
        :param path: 目录路径
        :param object_name: 对象名
        :return:
            文件存在：文件对象BucketFileInfo
            不存在：None
        '''
        with switch_collection(BucketFileInfo,
                               get_collection_name(bucket_name=bucket_name)):
            ok, file = bfm.get_file_exists(object_name)
            if ok:
                return file
        raise None


class GetFileObjectView(View):
    '''
    文件对象下载类视图
    '''
    def get(self, request, *args, **kwargs):
        bucket_name = kwargs.get('bucket_name')
        path = kwargs.get('path')
        object_name = kwargs.get('object_name')

        bfm = BucketFileManagement(path=path)
        with switch_collection(BucketFileInfo,
                               get_collection_name(bucket_name=bucket_name)):
            ok, file = bfm.get_file_exists(object_name)
            if not ok or not file:
                raise Http404('参数有误，未找到相关记录')

            response = self.get_file_download_response(str(file.id), file.na)
            if not response:
                raise Http404('要下载的文件不存在')
        return response

    def get_file_download_response(self, file_id, filename):
        '''
        获取文件下载返回对象
        :param file_id: 文件Id, type: str
        :filename: 文件名， type: str
        :return:
            success：http返回对象，type: dict；
            error: None
        '''
        fs = FileStorage(file_id)
        file_generator = fs.get_file_generator()
        if not file_generator:
            return None

        # response = StreamingHttpResponse(file_read_iterator(full_path_filename))
        response = FileResponse(file_generator())
        response['Content-Type'] = 'application/octet-stream'  # 注意格式
        response['Content-Disposition'] = f'attachment;filename="{filename}"'  # 注意filename 这个是下载后的名字
        return response


class DirectoryView(View):
    '''
    目录类视图
    '''
    def post(self, request, *args, **kwargs):
        pass
