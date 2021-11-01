from django.test import TestCase

from buckets.models import get_next_bucket_max_id, Bucket, Archive


class SomeTests(TestCase):
    def test_get_next_bucket_max_id(self):

        # no bucket and no archive
        next_id = get_next_bucket_max_id()
        self.assertEqual(next_id, 1)

        # has bucket and no archive
        pre_id = next_id
        bucket1 = Bucket(id=pre_id, name=f'test-{pre_id}')
        bucket1.save(force_insert=True)
        self.assertEqual(bucket1.id, pre_id)
        next_id = get_next_bucket_max_id()
        self.assertEqual(next_id, pre_id + 1)

        pre_id = next_id
        bucket2 = Bucket(id=pre_id, name=f'test-{pre_id}')
        bucket2.save(force_insert=True)
        self.assertEqual(bucket2.id, pre_id)
        next_id = get_next_bucket_max_id()
        self.assertEqual(next_id, pre_id + 1)

        # has bucket and has archive
        bucket2.delete_and_archive()
        next_id = get_next_bucket_max_id()
        self.assertEqual(next_id, pre_id + 1)

        pre_id = next_id
        bucket3 = Bucket(id=pre_id, name=f'test-{pre_id}')
        bucket3.save(force_insert=True)
        self.assertEqual(bucket3.id, pre_id)
        next_id = get_next_bucket_max_id()
        self.assertEqual(next_id, pre_id + 1)

        bucket3_id = bucket3.id
        bucket3.delete_and_archive()
        next_id = get_next_bucket_max_id()
        self.assertEqual(next_id, pre_id + 1)

        Archive.objects.get(original_id=bucket3_id).delete()
        next_id = get_next_bucket_max_id()
        self.assertEqual(next_id, bucket3_id)
