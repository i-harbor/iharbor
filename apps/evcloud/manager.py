# import os
# import sys
# import django

# # 将项目路径添加到系统搜寻路径当中，查找方式为从当前脚本开始，找到要调用的django项目的路径
# sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# # 设置项目的配置文件 不做修改的话就是 settings 文件
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
# django.setup()  # 加载项目配置

import requests
from evcloud.models import APIAuth


class evcloud_operations():

    def __init__(self, api = 'default'):
        # if not isinstance(api_config, APIAuth):
        #     if not isinstance(api_config, str):
        #         raise Exception("param api_config is invalid")
        #     api_config = get_api_config(api_config)
        #     if not api_config:
        #         raise Exception("Can't get api_config")
        if api == 'default':
            api_config = APIAuth.objects.first()
        else:
            api_config = APIAuth.objects.get(id = api)

        self.group_id = api_config.group_id
        self.vlan_id = api_config.vlan_id
        self.api_url = api_config.url
        self.center_id = api_config.center_id
        self.auth = (api_config.name, api_config.pwd)

    def get_image_list(self):
        result = requests.get(f'{self.api_url}image/?center_id={self.center_id}', auth=(self.auth)).json()
        finally_result = {}
        for i, image in enumerate(result['results']):
            finally_result[i] = image
        return finally_result

    def create(self, image, cpu, mem, remarks):
        data = {
            "image_id": image,
            "vcpu": cpu,
            "mem": mem,
            "vlan_id": self.vlan_id,
            "group_id": self.group_id,
            # "host_id": 0,
            "remarks": remarks,
        }
        vm = requests.post(f'{self.api_url}vms/', auth=(self.auth), data=data).json()
        # return self.read_vm(vm_id)
        return {'uuid': vm['vm']['uuid'],
                'ipv4': vm['vm']['mac_ip'],
                'group_id': vm['data']['group_id']}

    def get_status(self, vm_id):
        status_list = {0: 'no state',
                       1: '运行',
                       2: 'blocked',
                       3: 'paused',
                       4: 'shut down',
                       5: '关机',
                       6: 'crashed',
                       7: 'suspended',
                       8: '',
                       9: 'host connect failed',
                       10: 'miss',
                       }
        try:
            result = requests.get(f'{self.api_url}vms/{vm_id}/status/', auth=(self.auth)).json()
            if result['code'] == 200:
                return (200, status_list[result['status']['status_code']])
            else:
                return (400, result['code_text'])
        except Exception as e:
            return (400, str(e).encode('utf-8').decode('unicode_escape'))


    def operations(self, vm_id, vm_operate):
        operate_list = ['start', 'shutdown', 'poweroff', 'reboot']
        params = {
            "op": operate_list[vm_operate],
        }
        try:
            result = requests.patch(f'{self.api_url}vms/{vm_id}/operations/', auth=(self.auth), data=params).json()
            if result['code'] == 200:
                return (200, result['code_text'])
            else:
                return (400, result['code_text'])
        except Exception as e:
            return (400, str(e).encode('utf-8').decode('unicode_escape'))

    def delete(self, vm_id):
        try:
            result = requests.delete(f'{self.api_url}vms/{vm_id}/', auth=(self.auth)).json()
            if result['code'] == 200:
                return (200, result['code_text'])
            else:
                return (400, result['code_text'])
        except Exception as e:
            return (400, str(e).encode('utf-8').decode('unicode_escape'))

    def create_vnc(self, vm_id):
        try:
            result = requests.post(f'{self.api_url}vms/{vm_id}/vnc/', auth=(self.auth)).json()
            if result['code'] == 200:
                return (200, result['vnc']['url'])
            else:
                return (400, result['code_text'])
        except Exception as e:
            return (400, str(e).encode('utf-8').decode('unicode_escape'))


if __name__ == '__main__':
    pass
    # image_list = evcloud_operations(4).get_image_list()
    # print(image_list)

    # vm_id = evcloud_operations(4).create(image=1, cpu=2, mem=2048, remarks='test')
    # print(vm_id)

    # status = evcloud_operations(4).get_status('cd999d2be0d946659d4d7169a5e55b03')
    # print(status)

    # res = evcloud_operations(4).delete('cd999d2be0d946659d4d7169a5e55b03')
    # print(res)

    # res = evcloud_operations(4).create_vnc('8e95bbcd50814edd9021252383882b52')
    # print(res)

    # res = evcloud_operations(4).operations('8e95bbcd50814edd9021252383882b52', 0)
    # print(res)

    
    