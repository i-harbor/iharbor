import logging
from django.utils.translation import gettext_lazy, gettext as _
from rest_framework import status
from drf_yasg.utils import swagger_auto_schema, no_body
from drf_yasg import openapi

from utils.view import CustomGenericViewSet
from utils import storagers
from . import permissions
from . import handlers
from . import parsers


logger = logging.getLogger('django.request')


class V2ObjViewSet(CustomGenericViewSet):
    """
    文件对象视图集
    """
    queryset = {}
    permission_classes = [permissions.IsAuthenticatedOrBucketToken]
    lookup_field = 'objpath'
    lookup_value_regex = '.+'
    parser_classes = (parsers.FileUploadParser, )

    @swagger_auto_schema(
        operation_summary=gettext_lazy('分片上传文件对象'),
        request_body=openapi.Schema(
            title='对象二进制数据',
            type=openapi.TYPE_STRING,
            format=openapi.FORMAT_BINARY,
        ),
        manual_parameters=[
            openapi.Parameter(
                name='objpath', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("文件对象绝对路径"),
                required=True
            ),
            openapi.Parameter(
                name='offset', in_=openapi.IN_QUERY,
                type=openapi.TYPE_INTEGER,
                description=gettext_lazy("分片数据的写入偏移量"),
                required=True
            ),
            openapi.Parameter(
                name='reset', in_=openapi.IN_QUERY,
                type=openapi.TYPE_BOOLEAN,
                description=gettext_lazy("reset=true时，如果对象已存在，先重置对象大小为0再写入"),
                required=False
            ),
            openapi.Parameter(
                name='Content-MD5', in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("文件对象hex md5"),
                required=True
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                {
                  "chunk_offset": 0,
                  "chunk": null,
                  "chunk_size": 34,
                  "created": true
                }
            """
        }
    )
    def create_detail(self, request, *args, **kwargs):
        """
        通过对象绝对路径分片上传对象

            说明：
            * 必须提交标头Content-MD5、Content-Length，将根据提供的MD5值检查对象，如果不匹配，则返回错误。
            * 分片数据以二进制bytes格式直接填充请求体；
            * 小文件可以作为一个分片上传，大文件请自行分片上传，分片过大可能上传失败，建议分片大小16MB；对象上传支持部分上传，
              分片上传数据直接写入对象，已成功上传的分片数据永久有效且不可撤销，请自行记录上传过程以实现断点续传；
            * 文件对象已存在时，数据上传会覆盖原数据，文件对象不存在，会自动创建文件对象，并且文件对象的大小只增不减；
              如果覆盖（已存在同名的对象）上传了一个新文件，新文件的大小小于原同名对象，上传完成后的对象大小仍然保持
              原对象大小（即对象大小只增不减），如果这不符合你的需求，参考以下2种方法：
              (1)先尝试删除对象（对象不存在返回404，成功删除返回204），再上传；
              (2)访问API时，提交reset参数，reset=true时，再保存分片数据前会先调整对象大小（如果对象已存在），未提供reset参
                数或参数为其他值，忽略之。
              ## 特别提醒：切记在需要时只在上传第一个分片时提交reset参数，否者在上传其他分片提交此参数会调整对象大小，
              已上传的分片数据会丢失。

            注意：
            分片上传现不支持并发上传，并发上传可能造成脏数据，上传分片顺序没有要求，请一个分片上传成功后再上传另一个分片

            Http Code: 状态码200：上传成功无异常时，返回数据：
            {
              "created": true       # 上传第一个分片时，可用于判断对象是否是新建的，True(新建的)
            }
            >>Http Code: 400 401 403 404 500
            {
                "code": "BadDigest",   // InvalidDigest、AccessDenied、BadRequest、BucketLockWrite
                "message": "xxx"
            }
        """
        return handlers.V2ObjectHandler.post_part(view=self, request=request, kwargs=kwargs)

    @swagger_auto_schema(
        operation_summary=gettext_lazy('上传一个完整对象'),
        request_body=openapi.Schema(
            title='对象二进制数据',
            type=openapi.TYPE_STRING,
            format=openapi.FORMAT_BINARY,
        ),
        manual_parameters=[
            openapi.Parameter(
                name='objpath', in_=openapi.IN_PATH,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("文件对象绝对路径"),
                required=True
            ),
            openapi.Parameter(
                name='Content-MD5', in_=openapi.IN_HEADER,
                type=openapi.TYPE_STRING,
                description=gettext_lazy("文件对象hex md5"),
                required=True
            ),
        ],
        responses={
            status.HTTP_200_OK: """
                {
                    "created": true   # true: 上传创建一个新对象; false: 上传覆盖一个已存在的旧对象 
                }
                """
        }
    )
    def update(self, request, *args, **kwargs):
        """
        上传一个完整对象, 如果同名对象已存在，会覆盖旧对象；
        上传对象大小限制5GB，超过限制的对象请使用分片上传方式；
        不提供对象锁定，如果同时对同一对象发起多个写请求，会造成数据混乱，损坏数据一致性；

        * 必须提交标头Content-MD5、Content-Length，将根据提供的MD5值检查对象，如果不匹配，则返回错误。
        * 对象数据以二进制bytes格式直接填充请求体
        """
        return handlers.V2ObjectHandler.put_object(view=self, request=request, kwargs=kwargs)

    def get_parsers(self):
        """
        动态分配请求体解析器
        """
        method = self.request.method.lower()
        action = self.action_map.get(method)
        if action == 'update':
            return [parsers.NoNameFileUploadParser()]
        elif action == 'create_detail':
            self.request.upload_handlers = [
                storagers.AllFileUploadInMemoryHandler(request=self.request)
            ]       # DRF Session auth CRSF 会触发接收请求体，文件上传处理器要提前替换
            return [parsers.NoNameFileUploadParser()]

        return super().get_parsers()
