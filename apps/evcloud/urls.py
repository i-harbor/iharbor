from django.conf.urls import url
from django.contrib.auth.decorators import login_required

from . import views

urlpatterns = [
    url(r'^list/', login_required(views.evcloud_list), name='list'),
    url(r'^add/', login_required(views.evcloud_add), name='add'),

]


