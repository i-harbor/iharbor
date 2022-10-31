from rest_framework.parsers import FileUploadParser

from s3 import exceptions
from s3 import renders
from s3 import parsers
from s3.handlers.bucket import BucketHandler
from s3.negotiation import CusContentNegotiation
from s3.viewsets import S3CustomGenericViewSet
from s3.handlers.get_object import GetObjectHandler
from s3.handlers.head_object import HeadObjectHandler
from s3.handlers.delete_object import DeleteObjectHandler
from s3.handlers.put_object import PutObjectHandler
from s3.handlers.copy_object import CopyObjectHandler
from s3.handlers.multipart_upload import MultipartUploadHandler
from s3.models import MultipartUpload


class ObjViewSet(S3CustomGenericViewSet):
    http_method_names = ['get', 'post', 'put', 'delete', 'head', 'options']
    renderer_classes = [renders.CusXMLRenderer]
    content_negotiation_class = CusContentNegotiation
    parser_classes = [parsers.S3XMLParser]

    def list(self, request, *args, **kwargs):
        """
        get object
        """
        # GetObjectAcl
        if 'acl' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='GetObjectAcl not implemented'))

        # GetObjectLegalHold
        if 'legal-hold' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='GetObjectLegalHold not implemented'))

        # GetObjectLockConfiguration
        if 'object-lock' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='GetObjectLockConfiguration not implemented'))

        # GetObjectRetention
        if 'retention' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='GetObjectRetention not implemented'))

        # GetObjectTagging
        if 'tagging' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='GetObjectTagging not implemented'))

        # GetObjectTorrent
        if 'torrent' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='GetObjectTorrent not implemented'))

        # ListParts
        if 'uploadId' in request.query_params:
            # return self.exception_response(request, exceptions.S3NotImplemented(
            #     message='ListParts not implemented'))
            return MultipartUploadHandler().list_parts(request, view=self)

        return GetObjectHandler().s3_get_object(request=request, view=self)

    def create(self, request, *args, **kwargs):
        """
        CreateMultipartUpload
        CompleteMultipartUpload
        """
        uploads = request.query_params.get('uploads', None)
        if uploads is not None:
            # 创建多部分上传数据表
            try:
                MultipartUpload.create_table()
            except Exception as e:
                return self.exception_response(request, e)
            return MultipartUploadHandler().create_multipart_upload(request=request, view=self)
        upload_id = request.query_params.get('uploadId', None)
        if upload_id is not None:
            return MultipartUploadHandler().complete_multipart_upload(request=request, view=self, upload_id=upload_id)

        return self.exception_response(request, exceptions.S3MethodNotAllowed())

    def update(self, request, *args, **kwargs):
        """
        put object
        create dir
        upload part
        """
        key = self.get_s3_obj_key(request)
        content_length = request.headers.get('Content-Length', None)
        if not content_length:
            return self.exception_response(request, exceptions.S3MissingContentLength())

        if key.endswith('/') and content_length == '0':
            return PutObjectHandler.create_dir(request=request, view=self)

        # UploadPart check
        part_num = request.query_params.get('partNumber', None)
        upload_id = request.query_params.get('uploadId', None)
        if part_num is not None and upload_id is not None:
            return MultipartUploadHandler().upload_part(request=request, view=self)
            # return self.exception_response(request, exceptions.S3NotImplemented(
            #     message='UploadPart not implemented'))
            # if 'x-amz-copy-source-range' in request.headers or 'x-amz-copy-source' in request.headers:
            #     return self.exception_response(request, exceptions.S3NotImplemented(
            #         message='UploadPartCopy not implemented'))
            #
            # return self.upload_part(request=request, part_num=part_num, upload_id=upload_id)


        # PutObjectAcl
        if 'acl' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='PutObjectAcl not implemented'))

        # PutObjectLegalHold
        if 'legal-hold' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='PutObjectLegalHold not implemented'))

        # PutObjectLockConfiguration
        if 'object-lock' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='PutObjectLockConfiguration not implemented'))

        # PutObjectRetention
        if 'retention' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='PutObjectRetention not implemented'))

        # PutObjectTagging
        if 'tagging' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='PutObjectTagging not implemented'))

        if 'x-amz-copy-source' in request.headers:
            return CopyObjectHandler().copy_object(request=request, view=self)

        return PutObjectHandler().put_object(request=request, view=self)

    def destroy(self, request, *args, **kwargs):
        """
        delete object
        delete dir
        AbortMultipartUpload
        """
        upload_id = request.query_params.get('uploadId', None)
        if upload_id is not None:
            # return self.exception_response(request, exceptions.S3NotImplemented(
            #     message='ListParts not implemented'))
            return MultipartUploadHandler().abort_multipart_upload(request=request, view=self, upload_id=upload_id)

        key = self.get_s3_obj_key(request)
        if key.endswith('/'):
            return PutObjectHandler.delete_dir(request=request, view=self)

        return DeleteObjectHandler.delete_object(request=request, view=self)

    def head(self, request, *args, **kwargs):
        """
        head object
        """
        return HeadObjectHandler().head_object(request=request, view=self)

    def get_parsers(self):
        """
        动态分配请求体解析器
        """
        method = self.request.method.lower()
        if method == 'put':                     # put_object
            return [FileUploadParser()]

        return super().get_parsers()
