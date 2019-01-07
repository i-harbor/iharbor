from django.shortcuts import render

from .models import VPNUsageDescription

# Create your views here.

def usage(request, *args, **kwags):
    '''
    vpn使用说明视图
    '''
    article_usage = VPNUsageDescription.objects.first()
    return render(request, 'base_usage_article.html', context={'article': article_usage})
