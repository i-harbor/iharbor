from .security_settings import TEST_CASE_SECURITY

TEST_CASE = {
    'BACKUP_SERVER': {
        'PROVIDER': {
            'endpoint_url': 'http://10.102.50.2:8000/',
            'bucket_name': 'akk'
        }
    },

}

TEST_CASE.update(TEST_CASE_SECURITY)
