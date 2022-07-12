from django.urls import path
from django.conf import settings

from . import views

app_name = "users"

urlpatterns = [
    path('register/', views.register_user, name='register'),
    # path('signin/', views.sign_in, name='signin'),
    path('logout/', views.logout_user, name='logout'),
    path('change_password/', views.change_password, name='change_password'),
    path('active/', views.active_user, name='active'),
    path('forget/', views.forget_password, name='forget'),
    path('fconfirm/', views.forget_password_confirm, name='forget_confirm'),
    path('security/', views.security, name='security')
]

if getattr(settings, 'LOCAL_LOGIN_USE', False):
    urlpatterns.append(path('login/', views.login_user, name='local_login'))

