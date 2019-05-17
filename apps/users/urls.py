from django.conf.urls import url

from . import views

app_name = "users"

urlpatterns = [
    url(r'^register/$', views.register_user, name='register'),
    url(r'^login/$', views.login_user, name='login'),
    url(r'^logout/$', views.logout_user, name='logout'),
    url(r'^change_password/$', views.change_password, name='change_password'),
    url(r'^active/$', views.active_user, name='active'),
    url(r'^forget/$', views.forget_password, name='forget'),
    url(r'^fconfirm/$', views.forget_password_confirm, name='forget_confirm'),
    url(r'^security/$', views.security, name='security')
]
