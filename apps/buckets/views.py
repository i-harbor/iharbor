from django.shortcuts import render
from django.views import View

from .models import Bucket, ApiUsageDescription, BucketToken


class BucketView(View):
    '''
    存储桶类视图
    '''
    def get(self, request):
        content = self.get_content(request=request)
        return render(request, 'bucket.html', context=content)

    def get_content(self, request):
        content = {}
        # content['buckets'] = Bucket.objects.filter(user=request.user).all()
        return content


class BucketDetailView(View):
    """
    存储桶详情类视图
    """
    def get(self, request, *args, **kwargs):
        bucket_name = kwargs.get('bucket_name', '')
        bucket = Bucket.objects.filter(name=bucket_name).first()
        context = {'bucket': bucket}
        if bucket:
            tokens = BucketToken.objects.filter(bucket=bucket).all()
            context['tokens'] = tokens

        return render(request, 'bucket_detail.html', context=context)


class UsageView(View):
    '''
    API使用说明类视图
    '''
    def get(self, request):
        article = ApiUsageDescription.objects.filter(desc_for=ApiUsageDescription.DESC_API).first()
        return render(request, 'base_usage_article.html', context={'article': article})


class FTPUsageView(View):
    '''
    FTP使用说明类视图
    '''
    def get(self, request):
        article = ApiUsageDescription.objects.filter(desc_for=ApiUsageDescription.DESC_FTP).first()
        return render(request, 'base_usage_article.html', context={'article': article})


class S3ApiUsageView(View):
    """
    FTP使用说明类视图
    """
    def get(self, request):
        article = ApiUsageDescription.objects.filter(desc_for=ApiUsageDescription.DESC_S3_API).first()
        return render(request, 'base_usage_article.html', context={'article': article})


class SearchObjectView(View):
    """
    搜索对象类视图
    """
    def get(self, request):
        buckets = Bucket.objects.filter(user=request.user).all()
        return render(request, 'search_object.html', context={'buckets': buckets})
