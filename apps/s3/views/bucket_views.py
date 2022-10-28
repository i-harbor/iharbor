from s3 import renders
from s3.handlers.multipart_upload import MultipartUploadHandler
from s3.viewsets import S3CustomGenericViewSet
from s3 import exceptions
from s3.negotiation import CusContentNegotiation
from s3 import parsers
from s3.handlers.list_object import ListObjectsHandler
from s3.handlers.bucket import BucketHandler
from s3.handlers.delete_object import DeleteObjectHandler


class MainHostViewSet(S3CustomGenericViewSet):
    """
    主域名请求视图集
    """
    permission_classes = []

    def list(self, request, *args, **kwargs):
        """
        list buckets

        HTTP/1.1 200
        <?xml version="1.0" encoding="UTF-8"?>
        <ListAllMyBucketsResult>
           <Buckets>
              <Bucket>
                 <CreationDate>timestamp</CreationDate>
                 <Name>string</Name>
              </Bucket>
           </Buckets>
           <Owner>
              <DisplayName>string</DisplayName>
              <ID>string</ID>
           </Owner>
        </ListAllMyBucketsResult>
        """
        return BucketHandler.list_buckets(request=request, view=self)


class BucketViewSet(S3CustomGenericViewSet):
    http_method_names = ['get', 'post', 'put', 'delete', 'head', 'options']
    renderer_classes = [renders.CusXMLRenderer]
    content_negotiation_class = CusContentNegotiation
    parser_classes = [parsers.S3XMLParser]

    def list(self, request, *args, **kwargs):
        """
        list objects (v1 && v2)
        get object metadata
        ListMultipartUploads
        """
        uploads = request.query_params.get('uploads', None)
        if uploads is not None:
            # return self.exception_response(request, exceptions.S3NotImplemented(
            #     message='ListMultipartUploads not implemented now'))
            return MultipartUploadHandler().list_multipart_upload(request, view=self)

        list_type = request.query_params.get('list-type', '1')
        if list_type == '2':
            return ListObjectsHandler().list_objects_v2(request=request, view=self)

        # ListObjectVersions
        if 'versions' in request.query_params:
            return self.exception_response(request, exceptions.S3NotImplemented(
                message='ListObjectVersions not implemented'))

        return ListObjectsHandler().list_objects_v1(request=request, view=self)

    def create(self, request, *args, **kwargs):
        """
        DeleteObjects
        """
        delete = request.query_params.get('delete')
        if delete is not None:
            return DeleteObjectHandler.delete_objects(request=request, view=self)

        return self.exception_response(request, exceptions.S3MethodNotAllowed())

    def update(self, request, *args, **kwargs):
        """
        create bucket

        Headers:
            x-amz-acl:
                The canned ACL to apply to the bucket.
                Valid Values: private | public-read | public-read-write | authenticated-read
        """
        bucket_name = self.get_bucket_name(request)
        if not bucket_name:
            return self.exception_response(request, exceptions.S3InvalidRequest('Invalid request domain name'))

        return BucketHandler.create_bucket(request=request, view=self, bucket_name=bucket_name)

    def destroy(self, request, *args, **kwargs):
        """
        delete bucket
        """
        return BucketHandler.delete_bucket(request=request, view=self)

    def head(self, request, *args, **kwargs):
        """
        head bucket
        """
        return BucketHandler.head_bucket(request=request, view=self)

