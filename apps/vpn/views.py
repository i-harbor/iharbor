from django.shortcuts import render
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required

from .models import VPNUsageDescription, VPNAuth


# Create your views here.

def usage(request, *args, **kwags):
    '''
    vpn使用说明视图
    '''
    article_usage = VPNUsageDescription.objects.first()
    return render(request, 'base_usage_article.html', context={'article': article_usage})


@login_required()
def vpn(request, *args, **kwargs):
    user = request.user
    vpn, created = VPNAuth.objects.get_or_create(user=user)
    return render(request, 'vpn.html', context={'vpn': vpn})





