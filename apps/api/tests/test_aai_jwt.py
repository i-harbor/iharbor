from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

from api.authentications.aai.jwt import TokenBackend
from .tests import MyAPITestCase


class AAIJWTTests(MyAPITestCase):
    def setUp(self):
        self.rsa = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.private_key = self.rsa.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ).decode('utf-8')
        self.public_rsa = self.rsa.public_key()
        self.public_key = self.public_rsa.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')

    def test_jwt_token_backend(self):
        token_backend = TokenBackend(
            algorithm='RS512', signing_key=self.private_key, verifying_key=self.public_key,
            audience=None, issuer=None
        )
        token = token_backend.encode(
            payload={'username': 'test', 'company': 'cnic', 'age': 18},
            headers={'header1': 'header'}
        )

        r = token_backend.decode(token=token)
        payload = r['payload']
        headers = r['header']
        self.assertEqual(payload['username'], 'test')
        self.assertEqual(payload['company'], 'cnic')
        self.assertEqual(payload['age'], 18)
        self.assertEqual(headers['header1'], 'header')
