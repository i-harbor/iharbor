import base64

from rest_framework.response import Response

from utils.md5 import FileMD5Handler
from s3.viewsets import S3CustomGenericViewSet
from s3 import exceptions
from s3.harbor import HarborManager
from s3 import renders


class DeleteObjectHandler:
    @staticmethod
    def delete_objects(request, view: S3CustomGenericViewSet):
        bucket_name = view.get_bucket_name(request)

        body = request.body
        content_b64_md5 = view.request.headers.get('Content-MD5', '')
        md5_hl = FileMD5Handler()
        md5_hl.update(offset=0, data=body)
        bytes_md5 = md5_hl.digest()
        base64_md5 = base64.b64encode(bytes_md5).decode('ascii')
        if content_b64_md5 != base64_md5:
            return view.exception_response(request, exceptions.S3BadDigest())

        try:
            data = request.data
        except Exception as e:
            return view.exception_response(request, exceptions.S3MalformedXML())

        root = data.get('Delete')
        if not root:
            return view.exception_response(request, exceptions.S3MalformedXML())

        keys = root.get('Object')
        if not keys:
            return view.exception_response(request, exceptions.S3MalformedXML())

        # XML解析器行为有关，只有一个item时不是list
        if not isinstance(keys, list):
            keys = [keys]

        if len(keys) > 1000:
            return view.exception_response(request, exceptions.S3MalformedXML(
                message='You have attempted to delete more objects than allowed 1000'))

        deleted_objs, err_objs = HarborManager().delete_objects(
            bucket_name=bucket_name, obj_keys=keys, user=request.user)

        quiet = root.get('Quiet', 'false').lower()
        if quiet == 'true':     # 安静模式不包含 删除成功对象信息
            data = {'Error': err_objs}
        else:
            data = {'Error': err_objs, 'Deleted': deleted_objs}

        view.set_renderer(request, renders.CommonXMLRenderer(root_tag_name='DeleteResult'))
        return Response(data=data, status=200)

    @staticmethod
    def delete_object(request, view: S3CustomGenericViewSet):
        bucket_name = view.get_bucket_name(request)
        obj_path_name = view.get_obj_path_name(request)
        h_manager = HarborManager()
        try:
            h_manager.delete_object(bucket_name=bucket_name, obj_path=obj_path_name, user=request.user)
        except exceptions.S3Error as e:
            return view.exception_response(request, e)

        return Response(status=204)
