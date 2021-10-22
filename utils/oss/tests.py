import os
import unittest
import io
import random
import hashlib
from string import printable
from utils.md5 import offset_chunks

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webserver.settings")
from .pyrados import build_harbor_object, get_size


def random_string(length: int = 10):
    return random.choices(printable, k=length)


def random_bytes_io(mb_num: int):
    bio = io.BytesIO()
    for i in range(1024):           # MB
        s = ''.join(random_string(mb_num))
        b = s.encode() * 1024         # KB
        n = bio.write(b)

    bio.seek(0)
    return bio


def calculate_md5(data: bytes):
    md5obj = hashlib.md5()
    md5obj.update(data)
    return md5obj.hexdigest()


class TestHarborObject(unittest.TestCase):
    POOL_NAME = 'obs-test'
    USING = 'default'

    def build_data(self):
        data_io = random_bytes_io(60)  # 60MB
        data_io.seek(0)
        data_md5 = calculate_md5(data_io.read())
        return data_io, data_md5

    def test_rados(self):
        data_io, data_md5 = self.build_data()
        ho = build_harbor_object(using=self.USING, pool_name=self.POOL_NAME, obj_id='test_object')

        # write
        data_io.seek(0)
        chunk = data_io.read(64 * 1024 * 1024)
        ok, msg = ho.write(data_block=chunk)
        self.assertTrue(ok, msg=f'write rados error, {msg}')

        self.read_check_and_delete(data_io, data_md5, ho)

    def test_rados_file(self):
        data_io, data_md5 = self.build_data()
        ho = build_harbor_object(using=self.USING, pool_name=self.POOL_NAME, obj_id='test_object')

        # write file
        data_io.seek(0)
        ok, msg = ho.write_file(offset=0, file=data_io)
        self.assertTrue(ok, msg=f'write rados error, {msg}')

        self.read_check_and_delete(data_io, data_md5, ho)

    def test_write_generator(self):
        data_io, data_md5 = self.build_data()
        ho = build_harbor_object(using=self.USING, pool_name=self.POOL_NAME, obj_id='test_object')
        wg = ho.write_obj_generator()
        if next(wg):
            for chunk in offset_chunks(data_io):
                if not chunk:
                    break
                off, d = chunk
                if not wg.send((off, d)):
                    if not wg.send((off, d)):
                        raise self.failureException('test_write_generator failed')

        rg = ho.read_obj_generator()
        data = bytes()
        for d in rg:
            data += d

        r_md5 = calculate_md5(data)
        self.assertEqual(r_md5, data_md5, msg='test_write_generator；read rados md5 != write rados md5.')

        # delete
        data_size = get_size(data_io)
        ok, msg = ho.delete(obj_size=data_size)
        self.assertTrue(ok, msg='delete rados error.')

    def read_check_and_delete(self, data_io, data_md5, ho):
        # read
        data_size = get_size(data_io)
        ok, data = ho.read(offset=0, size=data_size)
        self.assertTrue(ok, msg=f'read rados error, {data}')
        r_md5 = calculate_md5(data)
        self.assertEqual(r_md5, data_md5, msg='read rados md5 != write rados md5.')

        # delete
        ok, msg = ho.delete(obj_size=data_size)
        self.assertTrue(ok, msg='delete rados error.')


if __name__ == '__main__':
    unittest.main()
