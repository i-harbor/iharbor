from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.file_list, name='file_list'),
    url(r'^download/(?P<uuid>[\w-]{32,36})', views.download, name='download'),
    url(r'^delete/(?P<uuid>[\w-]{32,36})', views.delete, name='delete'),
    # path('<int:blog_id>/', views.BlogDetailView.as_view(), name='blog_detail'),
    # url(r'^(?P<blog_id>\d+)/$', views.BlogDetailView.as_view(), name='blog_detail'),
    # url(r'^add/$', views.blog_add, name='blog_add')
]


