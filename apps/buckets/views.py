from django.shortcuts import render
from django.views import View
from django.db.models import Q as dQ

from .models import Bucket, ApiUsageDescription


# Create your views here.


class BucketView(View):
    '''
    存储桶类视图
    '''
    def get(self, request):
        content = self.get_content(request=request)
        return render(request, 'bucket.html', context=content)

    def get_content(self, request):
        content = {}
        content['buckets'] = Bucket.objects.filter(user=request.user).all()
        return content

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

