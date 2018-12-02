from django.shortcuts import render
from django.views import View
from django.db.models import Q as dQ

from .models import Bucket


# Create your views here.


class BucketView(View):
    '''
    存储桶类视图
    '''
    def get(self, request):
        content = self.get_content(request=request)
        return render(request, 'bucket.html', context=content)
        # return render(request, 'base_with_sidebar.html', context=content)

    def get_content(self, request):
        content = {}
        content['buckets'] = Bucket.objects.filter(dQ(user=request.user) & dQ(soft_delete=False)).all()
        return content


