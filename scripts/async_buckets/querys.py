import threading
from functools import wraps
from datetime import timedelta, datetime
from pytz import utc

from .databases import get_connection, meet_async_timedelta_minutes


METADATA = 'metadata'
DEFAULT = 'default'


_metadata_db_lock = threading.Lock()


def db_write_lock(func):
    @wraps(func)
    def wrapper(*arge, **kwargs):
        _metadata_db_lock.acquire()
        try:
            return func(*arge, **kwargs)
        except Exception as e:
            raise e
        finally:
            _metadata_db_lock.release()

    return wrapper


class BackupNum:
    ONE = 1
    TWO = 2

    @classmethod
    def values(cls):
        return [cls.ONE, cls.TWO]


def is_naive(value):
    """
    Determine if a given datetime.datetime is naive.
    """
    return value.utcoffset() is None


def timezone_now():
    return datetime.utcnow().replace(tzinfo=utc)


def make_naive(value, timezone=utc):
    """Make an aware datetime.datetime naive in a given time zone."""
    if is_naive(value):
        return value

    return value.astimezone(timezone).replace(tzinfo=None)


def db_datetime_str(dt: datetime):
    if not is_naive(dt):
        dt = make_naive(dt)

    return str(dt)


class QueryHandler:
    MEET_ASYNC_TIMEDELTA_MINUTES = meet_async_timedelta_minutes

    def select(self,using: str, sql: str, result_type: str = 'all'):
        """
        :param result_type: one, all
        :return:
            (dict, )            # when result_type == all
            dict or None        # when result_type == one
        :raises: Exception
        """
        if result_type not in ['one', 'all']:
            raise Exception('invalid value of param result_type')

        conn = get_connection(using)
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                if result_type == 'one':
                    ret = cursor.fetchone()
                elif result_type == 'all':
                    ret = cursor.fetchall()

                return ret
        except Exception as e:
            raise e

    def select_one(self,using: str, sql: str):
        """
        :return:
            dict or None
        """
        return self.select(using=using, sql=sql, result_type='one')

    def select_all(self,using: str, sql: str):
        return self.select(using=using, sql=sql, result_type='all')

    @db_write_lock
    def update(self,using: str, sql: str):
        try:
            conn = get_connection(using)
            with conn.cursor() as cursor:
                rows = cursor.execute(sql)
                conn.commit()
                return rows
        except Exception as e:
            raise e

    @staticmethod
    def _bucket_table_name(bucket_id):
        return f'bucket_{bucket_id}'

    @staticmethod
    def get_need_async_bucket_query_sql(id_gt: int = 0, limit: int = 1000, names: list = None):
        """
        获取设置了备份点并开启了备份的所有桶sql, id正序排序

        :param id_gt: 查询id大于id_gt的数据，实现分页续读
        :param limit: 获取数据的数量
        :param names: bucket name list
        :return:
            str
        """
        fields = [
            '`buckets_bucket`.`id`',
            '`buckets_bucket`.`access_permission`',
            '`buckets_bucket`.`user_id`',
            '`buckets_bucket`.`objs_count`',
            '`buckets_bucket`.`size`',
            '`buckets_bucket`.`stats_time`',
            '`buckets_bucket`.`ftp_enable`',
            '`buckets_bucket`.`ftp_password`',
            '`buckets_bucket`.`ftp_ro_password`',
            '`buckets_bucket`.`pool_name`',
            '`buckets_bucket`.`type`',
            '`buckets_bucket`.`ceph_using`',
            '`buckets_bucket`.`name`',
            '`buckets_bucket`.`created_time`',
            '`buckets_bucket`.`collection_name`',
            '`buckets_bucket`.`modified_time`',
            '`buckets_bucket`.`remarks`',
            '`buckets_bucket`.`lock`'
        ]
        fields_sql = ','.join(fields)
        inner_join = "INNER JOIN `buckets_backupbucket` ON (`buckets_bucket`.`id` = `buckets_backupbucket`.`bucket_id`)"
        where = f"`buckets_backupbucket`.`status` = 'start' AND `buckets_bucket`.`id` > {id_gt}"
        if names:
            in_names = ', '.join([f'"{n}"' for n in names])
            where += f" AND `buckets_bucket`.`name` IN ({in_names})"

        sql = f'SELECT {fields_sql} FROM `buckets_bucket` {inner_join} WHERE ({where}) ' \
              f'ORDER BY `buckets_bucket`.`id` ASC LIMIT {limit}'
        return sql

    def get_need_async_buckets(self, id_gt: int = 0, limit: int = 10, names: list = None):
        """
        获取设置了备份点并开启了备份的所有桶, id正序排序

        :param id_gt: 查询id大于id_gt的数据，实现分页续读
        :param limit: 获取数据的数量
        :param names: bucket name list
        :return:
            list
        """
        query_hand = QueryHandler()
        sql = query_hand.get_need_async_bucket_query_sql(
            id_gt=id_gt, limit=limit, names=names
        )
        return self.select_all(using=DEFAULT, sql=sql)

    def get_need_async_objects_query_sql(self, bucket_id: int, id_gt: int, limit: int, backup_nums: list,
                                         meet_time=None, id_mod_div: int = None, id_mod_equal: int = None):
        """
        获取需要同步的对象的查询sql, id正序排序

        :param bucket_id: bucket id
        :param id_gt: 查询id大于id_gt的数据，实现分页续读
        :param limit: 获取数据的数量
        :param meet_time: 查询upt大于此时间的对象
        :param id_mod_div: object id求余的除数，和参数id_mod_equal一起使用
        :param id_mod_equal: object id求余的余数相等的筛选条件，仅在参数id_mod有效时有效
        :param backup_nums: 筛选条件，只查询指定备份点编号需要同步的对象，[int, ]
        :return:
            QuerySet
        """
        if not backup_nums:
            return []

        table_name = self._bucket_table_name(bucket_id)
        fields = [
            f'`{table_name}`.`id`',
            f'`{table_name}`.`na`',
            f'`{table_name}`.`na_md5`',
            f'`{table_name}`.`name`',
            f'`{table_name}`.`fod`',
            f'`{table_name}`.`did`',
            f'`{table_name}`.`si`',
            f'`{table_name}`.`ult`',
            f'`{table_name}`.`upt`',
            f'`{table_name}`.`dlc`',
            f'`{table_name}`.`shp`',
            f'`{table_name}`.`stl`',
            f'`{table_name}`.`sst`',
            f'`{table_name}`.`set`',
            f'`{table_name}`.`sds`',
            f'`{table_name}`.`md5`',
            f'`{table_name}`.`share`',
            f'`{table_name}`.`async1`',
            f'`{table_name}`.`async2`'
        ]
        fields_sql = ','.join(fields)

        where_list = [f"`fod` AND `id` > {id_gt}"]
        if meet_time is None:
            meet_time = self._get_meet_time()

        meet_time_str = db_datetime_str(meet_time)
        where_list.append(f"(`upt` < '{meet_time_str}' OR `upt` IS NULL)")

        num_where_items = []
        if 1 in backup_nums:
            num_where_items.append("`async1` IS NULL OR `upt` > `async1`")

        if 2 in backup_nums:
            num_where_items.append("`async2` IS NULL OR `upt` > `async2`")

        if num_where_items:
            num_where = ' OR '.join(num_where_items)
            where_list.append(f"({num_where})")

        if id_mod_div is not None and id_mod_equal is not None:
            if id_mod_div >= 1 and (0 <= id_mod_equal < id_mod_div):
                where_list.append(f"MOD(`id`, {id_mod_div}) = {id_mod_equal}")

        where = " AND ".join(where_list)
        sql = f"SELECT {fields_sql} FROM `{table_name}` WHERE ({where}) ORDER BY `id` ASC LIMIT {limit}"

        return sql

    def get_need_async_objects(self, bucket_id, id_gt: int = 0, limit: int = 100, meet_time=None,
                               id_mod_div: int = None, id_mod_equal: int = None, backup_nums: list = None):
        """
        获取需要同步的对象, id正序排序

        :param bucket_id: bucket id
        :param id_gt: 查询id大于id_gt的数据，实现分页续读
        :param limit: 获取数据的数量
        :param meet_time: 查询upt大于此时间的对象
        :param id_mod_div: object id求余的除数，和参数id_mod_equal一起使用
        :param id_mod_equal: object id求余的余数相等的筛选条件，仅在参数id_mod有效时有效
        :param backup_nums: 筛选条件，只查询指定备份点编号需要同步的对象，
        :return: list
        """
        only_backup_nums = BackupNum.values()
        if backup_nums:
            for num in backup_nums:
                if num not in only_backup_nums:
                    raise Exception(f'Invalid backup_nums({backup_nums}), backup num only in {only_backup_nums}')
        else:
            backup_nums = only_backup_nums

        query_hand = QueryHandler()
        sql = query_hand.get_need_async_objects_query_sql(
            bucket_id=bucket_id, id_gt=id_gt, limit=limit, meet_time=meet_time, id_mod_div=id_mod_div,
            id_mod_equal=id_mod_equal, backup_nums=backup_nums
        )
        return self.select_all(using=METADATA, sql=sql)

    def _get_meet_time(self):
        return datetime.utcnow() - timedelta(minutes=self.MEET_ASYNC_TIMEDELTA_MINUTES)

    def update_object_async_time(self, bucket_id, obj_id, async_time, backup_num):
        async_time_str = db_datetime_str(async_time)
        table_name = self._bucket_table_name(bucket_id)

        if backup_num == BackupNum.ONE:
            update_field = 'async1'
        else:
            update_field = 'async2'

        sql = f"UPDATE `{table_name}` SET `{update_field}` = '{async_time_str}' WHERE `id` = {obj_id}"
        rows = self.update(using=METADATA, sql=sql)
        if rows == 1:
            return True

        return False

    def get_bucket_backup_sql(self, bucket_id, backup_num: int):
        """

        INNER JOIN `buckets_bucket` ON (`bucket_id` = `buckets_bucket`.`id`)
        """
        fields = [
            '`buckets_backupbucket`.`id`',
            '`buckets_backupbucket`.`bucket_id`',
            '`buckets_backupbucket`.`endpoint_url`',
            '`buckets_backupbucket`.`bucket_token`',
            '`buckets_backupbucket`.`bucket_name`',
            'buckets_backupbucket.`created_time`',
            '`buckets_backupbucket`.`modified_time`',
            '`buckets_backupbucket`.`remarks`',
            '`buckets_backupbucket`.`status`',
            '`buckets_backupbucket`.`backup_num`',
            '`buckets_backupbucket`.`error`'
        ]
        fields_sql = ', '.join(fields)

        where = f"`buckets_backupbucket`.`backup_num` = {backup_num} AND " \
                f"`buckets_backupbucket`.`bucket_id` = {bucket_id}"
        sql = f"SELECT {fields_sql} FROM `buckets_backupbucket` WHERE ({where}) " \
              f"ORDER BY `buckets_backupbucket`.`id` DESC LIMIT 1"
        return sql

    def get_bucket_backup(self, bucket_id, backup_num: int):
        sql = self.get_bucket_backup_sql(bucket_id=bucket_id, backup_num=backup_num)
        return self.select_one(using=DEFAULT, sql=sql)

    def update_backup_start(self, backup_id):
        sql = f"UPDATE `buckets_backupbucket` SET `status` = 'start' WHERE `id` = {backup_id}"
        rows = self.update(using=DEFAULT, sql=sql)
        if rows == 1:
            return True

        return False
