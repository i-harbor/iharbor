from django.core.management.base import BaseCommand, CommandError

from buckets.utils import BucketFileManagement
from buckets.models import Bucket


class Command(BaseCommand):
    """
    根据 桶 获取 ceph_uing 更新 对象的 pool_id
    只为对象文件增加字 pool_id 段
    """
    error_buckets = []
    # 需手动修改, {'alias1': 1, 'alias2': 2}
    ceph_pool_id_mapping = {'ceph2': 2, 'default': 1}

    help = """
            ** manage.py bucketfilepoolid --all [--id-gt=xxx] **
            ** manage.py bucketfilepoolid --bucket-name=xxx **
           """

    def add_arguments(self, parser):
        parser.add_argument(
            '--bucket-name', default=None, dest='bucketname',
            help='Name of bucket will',
        )
        parser.add_argument(
            '--all', default=None, nargs='?', dest='all', const=True, # 当命令行有此参数时取值const, 否则取值default
            help='exec sql for all buckets',
        )
        parser.add_argument(
            '--id-gt', default=0, dest='id-gt', type=int,
            help='All buckets with ID greater than "id-gt".',
        )

    def handle(self, *args, **options):
        print(options)
        buckets = self.get_buckets(**options)

        if not self.ceph_pool_id_mapping:
            raise CommandError("The attribute 'ceph_pool_id_mapping' needs to be set.")

        self.stdout.write(self.style.NOTICE(f'pool_id mapping: {self.ceph_pool_id_mapping}'))
        if input('Please verify the "pool_id mapping" \n\n '
                 + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("cancelled.")

        if input('Are you sure you want to do this? \n\n'
                 + "Type 'yes' to continue, or 'no' to cancel: ") != 'yes':
            raise CommandError("cancelled.")

        self.modify_buckets(buckets)

    def _get_buckets(self, **options):
        """
        获取给定的bucket或所有bucket
        :param options:
        :return:
        """
        bucketname = options['bucketname']
        all_ = options['all']
        id_gt = options['id-gt']

        # 指定名字的桶
        if bucketname:
            self.stdout.write(self.style.NOTICE('Buckets named {0}'.format(bucketname)))
            return Bucket.objects.filter(name=bucketname).all()

        # 全部的桶
        if all_ is not None:
            self.stdout.write(self.style.NOTICE('All buckets.'))
            qs = Bucket.objects.all()
            if id_gt > 0:
                qs = qs.filter(id__gt=id_gt)

            return qs

        # 未给出参数
        if not bucketname:
            bucketname = input('Please input a bucket name:')

        self.stdout.write(self.style.NOTICE('Bucket named {0}'.format(bucketname)))
        return Bucket.objects.filter(name=bucketname).all()

    def get_buckets(self, **options):
        buckets = self._get_buckets(**options)
        buckets = buckets.order_by('id')
        return buckets

    def modify_one_bucket(self, bucket):
        """
        为一个bucket执行sql

        :param bucket: Bucket obj
        :return:
        pool_id 存在跳过  存在检测
        """
        ret = True
        table_name = bucket.get_bucket_table_name()
        table_name = f'`{table_name}`'

        if bucket.ceph_using not in self.ceph_pool_id_mapping:
            self.stdout.write(
                self.style.ERROR(
                    f"Bucket(id={bucket.id}, name={bucket.name}), bucket's ceph_using=“{bucket.ceph_using}” not in "
                    f"“{self.ceph_pool_id_mapping}”，Unable to know pool_id."))
            return False

        pool_id = self.ceph_pool_id_mapping[bucket.ceph_using]

        try:
            sql = f"ALTER TABLE {table_name} ADD `pool_id` INT(4) DEFAULT {pool_id};"
        except Exception as e:
            ret = False
            self.stdout.write(self.style.ERROR(
                f"Bucket(id={bucket.id}, name={bucket.name}), error when format sql: {str(e)}"))
        else:
            bfm = BucketFileManagement(collection_name=table_name)
            model_class = bfm.get_obj_model_class()
            try:
                a = model_class.objects.raw(raw_query=sql)
                b = a.query._execute_query()
            except Exception as e:
                ret = False
                self.error_buckets.append(bucket.name)
                if e.args[0] == 1060:
                    self.stdout.write(
                        self.style.WARNING(f"Bucket(id={bucket.id}, name={bucket.name}), warning appear {e.args[1]}，"
                                           f"-- sql: {sql} "))
                else:
                    self.stdout.write(self.style.ERROR(
                        f"Bucket(id={bucket.id}, name={bucket.name}), error when add field 'pool_id'"
                        f":{type(e)} {str(e)} -- sql: {sql}"))
            else:
                self.stdout.write(self.style.SUCCESS(
                    f"Bucket(id={bucket.id}, name={bucket.name})，success to add field 'pool_id'; sql: {sql} "))

        return ret

    def modify_buckets(self, buckets):
        """
        单线程为bucket执行sql
        :param buckets:
        :return: None
        """
        count = 0
        for bucket in buckets:
            ok = self.modify_one_bucket(bucket)
            if ok:
                count += 1
            else:
                self.stdout.write(self.style.ERROR(
                    f"Bucket(id={bucket.id}, name={bucket.name}), error when add field 'pool_id'."))
                break

        self.stdout.write(
            self.style.SUCCESS(
                'Successfully exec sql for {0} buckets'.format(count)
            )
        )
