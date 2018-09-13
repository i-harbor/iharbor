from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^$', views.file_list, name='file_list'),
    # path('<int:blog_id>/', views.BlogDetailView.as_view(), name='blog_detail'),
    # url(r'^(?P<blog_id>\d+)/$', views.BlogDetailView.as_view(), name='blog_detail'),
    # url(r'^add/$', views.blog_add, name='blog_add')
]


