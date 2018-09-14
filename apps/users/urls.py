from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^register/$', views.register_user, name='register'),
    url(r'^login/$', views.login_user, name='login'),
    url(r'^logout/$', views.logout_user, name='logout'),
    url(r'^change_password/$', views.change_password, name='change_password'),
    # path('<int:blog_id>/', views.BlogDetailView.as_view(), name='blog_detail'),
    # url(r'^(?P<blog_id>\d+)/$', views.BlogDetailView.as_view(), name='blog_detail'),
    # url(r'^add/$', views.blog_add, name='blog_add')
]
