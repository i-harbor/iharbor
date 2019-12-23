from django.urls import path

from . import views

app_name = "vpn"

urlpatterns = [
    path('', views.vpn, name='home'),
    path('usage/', views.usage, name='usage'),
]
