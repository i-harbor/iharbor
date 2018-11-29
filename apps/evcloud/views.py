from django.shortcuts import render
from django.http import JsonResponse
import json
import datetime
from .models import EvcloudVM
from .utils import evcloud_operations
vms = evcloud_operations()
# Create your views here.

def evcloud_list(request):
    if request.method == "GET":
        image_list = ['centos7 64bit', 'win10 64bit', 'centos6 64bit', 'winxp 32bit', 'fedora28 64bit']
        user = request.user
        vm_list = EvcloudVM.objects.filter(user=user).values()
        vm_list_dict = {}
        for i, vm in enumerate(vm_list):
            vm['vm_id_display'] = vm['vm_id'][-6:]
            vm['vm_image_display'] = image_list[int(vm['vm_image'])-1]
            vm['created_time_display'] = vm['created_time'].strftime("%Y-%m-%d")
            vm['end_time_display'] = vm['end_time'].strftime("%Y-%m-%d")
            #vm['status'] = vms.get_status(vm['vm_id'])
            vm_list_dict[i] = vm
        return render(request, 'evcloud_list.html', {'vm_list_dict':vm_list_dict})
    elif request.method == "POST":
        vm_id = request.POST.get('vm_id')
        vm_operate = int(request.POST.get('vm_operate'))
        if vm_operate == 4:
            code, e = vms.delete(vm_id)
            status = 'ok'
        elif vm_operate == 5:
            code, e = vms.create_vnc(vm_id)
            status = 'ok'
        elif vm_operate == 6:
            code, e = vms.get_status(vm_id)
            status = 'ok'
        elif 0 < vm_operate < 3:
            code, e = vms.operations(vm_id, vm_operate)
            status = '关机'
        else:
            code, e = vms.operations(vm_id, vm_operate)
            status = '开机'
        result = {
            'code': code,
            'status': status,
            'e': e,
        }
        #print(e)
        return JsonResponse(data=result)
def evcloud_add(request):
    #print(request.method)
    user = request.user
    if request.method == "GET":
        return render(request, 'evcloud_add.html')

    elif request.method == "POST":
        result = {}
        image = int(request.POST.get('image'))
        configure = int(request.POST.get('configure'))
        if configure == 1:
            cpu, mem = 2, 2048
        elif configure == 2:
            cpu, mem = 4, 4096
        else:
            cpu, mem = 8, 8192
        try:
            create_result = vms.create(image, cpu, mem)
            EvcloudVM.objects.create(vm_id=create_result['uuid'],
                                     user=user,
                                     end_time=datetime.datetime.now()+datetime.timedelta(days=365),
                                     vm_image=image,
                                     vm_cpu=cpu,
                                     vm_mem=mem,
                                     vm_ip=create_result['ipv4'],
                                     group_id=create_result['group_id'])
            #print(create_result)
            result['code'] = 200
        except Exception as e:
            result['code'] = 400
            result['error_text'] = str(e).encode('utf-8').decode('unicode_escape')
        return JsonResponse(data = result)
    else:
        return JsonResponse(data = 'error')