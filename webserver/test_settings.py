from .security_settings import TEST_CASE_SECURITY

TEST_CASE = {
    'BACKUP_SERVER': {
        'PROVIDER': {
            'endpoint_url': 'http://10.102.50.2:8000/',
            'bucket_name': 'akk'
        }
    },
    'CEPH_CLUSTER': {
        'alias': 'default',
        'cluster_name': 'ceph',
        'username': 'client.developer',
        'config_filename': '/home/uwsgi/iharbor/data/ceph/conf/default.conf',
        'keyring_filename': '/home/uwsgi/iharbor/data/ceph/conf/default.keyring',
        'pool_names': ["obs_test"],
        'disable_choice': False,                # True: 创建bucket时不选择；
    }
}

TEST_CASE.update(TEST_CASE_SECURITY)
