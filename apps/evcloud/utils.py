import coreapi

class evcloud_operations():

    def __init__(self):
        auth = coreapi.auth.BasicAuthentication(username='evcloud', password='evcloud')
        self.client = coreapi.Client(auth=auth)
        # self.schema = self.client.get("http://10.0.200.201/api/v2/docs/")

    def create(self, image, cpu, mem):
        action = ["vms", "create"]
        params = {
            "image_id": image,
            "vcpu": cpu,
            "mem": mem,
            "group_id": 1,
            "host_id": 3,
            "net_type_id": 1,
            "vlan_id": 2,
            "diskname": '',
            "remarks": '',
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