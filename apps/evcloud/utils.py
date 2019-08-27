import coreapi
from .models import APIAuth

# API_CONFIG_DICT = {}
# API_CONFIG_NAME_DEFAULT = 'default'
# def get_api_config(config_name=API_CONFIG_NAME_DEFAULT):
#     '''
#     :param config_name: 配置对象名
#     :return:
#         正常：APIAuth()
#         错误：None
#     '''
#     if config_name not in API_CONFIG_DICT:
#         api_cfg = APIAuth.objects.first()
#         if not api_cfg:
#             return None
#
#         API_CONFIG_DICT[config_name] = api_cfg
#
#     return API_CONFIG_DICT.get(config_name)


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
        self.pool_id = api_config.pool_id
        auth = coreapi.auth.BasicAuthentication(username=api_config.name, password=api_config.pwd)
        self.client = coreapi.Client(auth=auth)
        self.schema = self.client.get(api_config.url)

    def get_image_list(self):
        action = ["images", "list"]
        params = {
            "pool_id": self.pool_id,
        }
        search_result = self.client.action(self.schema, action, params=params)
        finally_result = {}
        for i, image in enumerate(search_result):
            finally_result[i] = image
        return finally_result

    def create(self, image, cpu, mem, remarks):
        action = ["vms", "create"]
        params = {
            "image_id": image,
            "vcpu": cpu,
            "mem": mem,
            "group_id": self.group_id,
            "vlan_id": self.vlan_id,
            "remarks": remarks,
        }
        vm_id = self.client.action(self.schema, action, params=params)
        return self.read_vm(vm_id)

    def read_vm(self, vm_id):
        action = ["vms", "read"]
        params = {
            "vm_id": vm_id,
        }
        return self.client.action(self.schema, action, params=params)

    def get_status(self, vm_id):
        status_list = ['运行', '2', '3', '4', '关机']
        action = ["vms", "status", "list"]
        params = {
            "vm_id": vm_id,
        }
        try:
            result = status_list[int(self.client.action(self.schema, action, params=params)) - 1]
            return (200, result)
        except Exception as e:
            return (400, str(e).encode('utf-8').decode('unicode_escape'))


    def operations(self, vm_id, vm_operate):
        operate_list = ['start', 'shutdown', 'poweroff', 'reboot']
        action = ["vms", "operations", "partial_update"]
        params = {
            "vm_id": vm_id,
            "op": operate_list[vm_operate],
        }
        try:
            self.client.action(self.schema, action, params=params)
            return (200, '')
        except Exception as e:
            return (400, str(e).encode('utf-8').decode('unicode_escape'))

    def delete(self, vm_id):
        action = ["vms", "delete"]
        params = {
            "vm_id": vm_id,
        }
        try:
            self.client.action(self.schema, action, params=params)
            return (200, '')
        except Exception as e:
            return (400, str(e).encode('utf-8').decode('unicode_escape'))

    def create_vnc(self, vm_id):
        action = ["vms", "vnc", "create"]
        params = {
            "vm_id": vm_id,
        }
        try:
            result = self.client.action(self.schema, action, params=params)
            return (200, result['url'])
        except Exception as e:
            return (400, str(e).encode('utf-8').decode('unicode_escape'))