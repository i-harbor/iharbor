from .security_settings import TEST_CASE_SECURITY

TEST_CASE = {
    'BACKUP_SERVER': {
        'PROVIDER': {
            'endpoint_url': 'http://10.102.50.2:8000/',
            'bucket_name': 'akk'
        }
    },
    # 结构修改测试需手动修改
    'CEPH_CLUSTER': {
        '3': {
            'name': 'obstest',
            'cluster_name': 'ceph',
            'username': 'client.admin',
            'config_filename': '/home/uwsgi/iharbor/data/ceph/conf/3.conf',
            'keyring_filename': '/home/uwsgi/iharbor/data/ceph/conf/3.keyring',
            'pool_names': ["obstest"],
            'disable_choice': False,  # True: 创建bucket时不选择；
            'priority_stored_value': 1,
        },
        '4': {
            'name': 'obstest2',
            'cluster_name': 'ceph',
            'username': 'client.admin',
            'config_filename': '/home/uwsgi/iharbor/data/ceph/conf/4.conf',
            'keyring_filename': '/home/uwsgi/iharbor/data/ceph/conf/4.keyring',
            'pool_names': ["obstest2"],
            'disable_choice': False,  # True: 创建bucket时不选择；
            'priority_stored_value': 2,
        }

    }

}

TEST_CASE.update(TEST_CASE_SECURITY)
