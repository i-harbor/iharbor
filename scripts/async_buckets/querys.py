from datetime import timedelta, datetime
from pytz import utc

from .databases import (
    get_connection, meet_async_timedelta_minutes,
    METADATA, DEFAULT, db_readwrite_lock
)


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


def quote_name(name):
    """
    :return: str
        `name`
    """
    if name.startswith("`") and name.endswith("`"):
        return name

    return f"`{name}`"


def table_columns(table_name: str):
    def _table_columns(column: str, _table_name: str = table_name):
        """
        :return: str
            `tablename`.`column`
        """
        _table_name = quote_name(_table_name)
        column = quote_name(column)
        return f"{_table_name}.{column}"

    return _table_columns


class QueryHandler:
    MEET_ASYNC_TIMEDELTA_MINUTES = meet_async_timedelta_minutes

    @db_readwrite_lock
    def select(self, using: str, sql: str, result_type: str = 'all'):
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
            conn.errors_occurred = True
            raise e

    def select_one(self, using: str, sql: str):
        """
        :return:
            dict or None
        """
        return self.select(using=using, sql=sql, result_type='one')

    def select_all(self, using: str, sql: str):
        return self.select(using=using, sql=sql, result_type='all')

    @db_readwrite_lock
    def update(self, using: str, sql: str):
        conn = get_connection(using)
        try:
            with conn.cursor() as cursor:
                rows = cursor.execute(sql)
                conn.commit()
                return rows
        except Exception as e:
            conn.errors_occurred = True
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
        table_name = 'buckets_bucket'
        tc = table_columns(table_name=table_name)
        qn = quote_name
        fields = [
            tc('id'),
            tc('access_permission'),
            tc('user_id'),
            tc('objs_count'),
            tc('size'),
            tc('stats_time'),
            tc('ftp_enable'),
            tc('ftp_password'),
            tc('ftp_ro_password'),
            tc('pool_name'),
            tc('type'),
            tc('ceph_using'),
            tc('name'),
            tc('created_time'),
            tc('collection_name'),
            tc('modified_time'),
            tc('remarks'),
            tc('lock')
        ]
        fields_sql = ', '.join(fields)

        backup_table = 'buckets_backupbucket'
        backup_tc = table_columns(table_name=backup_table)
        inner_join = f"INNER JOIN {qn(backup_table)} ON ({tc('id')} = {backup_tc('bucket_id')})"
        where = f"{backup_tc('status')} = 'start' AND {tc('id')} > {id_gt}"
        if names:
            in_names = ', '.join([f'"{n}"' for n in names])
            where += f" AND {tc('name')} IN ({in_names})"

        sql = f'SELECT {fields_sql} FROM {qn(table_name)} {inner_join} WHERE ({where}) ' \
              f'ORDER BY {tc("id")} ASC LIMIT {limit}'
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

    def get_need_async_objects_query_sql(
            self, bucket_id: int, id_gt: int, limit: int, backup_nums: list,
            meet_time=None, id_mod_div: int = None, id_mod_equal: int = None,
            size_gte: int = None
    ):
        """
        获取需要同步的对象的查询sql

            * id_gt和size_gte不能同时使用
            * 默认按id正序排序；
            * 当size_gte有效时，先按object size正序，后按id正序排序；

        :param bucket_id: bucket id
        :param id_gt: 查询id大于id_gt的数据，实现分页续读
        :param limit: 获取数据的数量
        :param meet_time: 查询upt大于此时间的对象
        :param id_mod_div: object id求余的除数，和参数id_mod_equal一起使用
        :param id_mod_equal: object id求余的余数相等的筛选条件，仅在参数id_mod有效时有效
        :param backup_nums: 筛选条件，只查询指定备份点编号需要同步的对象，[int, ]
        :param size_gte: 查询object size大于等于size_gte的数据
        :return:
            QuerySet
        """
        if id_gt is not None and size_gte is not None:
            raise ValueError('id_gt和size_gte不能同时使用')

        if not backup_nums:
            return []

        table_name = self._bucket_table_name(bucket_id)
        tc = table_columns(table_name=table_name)
        qn = quote_name
        fields = [
            tc('id'),
            tc('na'),
            tc('na_md5'),
            tc('name'),
            tc('fod'),
            tc('did'),
            tc('si'),
            tc('ult'),
            tc('upt'),
            tc('dlc'),
            tc('shp'),
            tc('stl'),
            tc('sst'),
            tc('set'),
            tc('sds'),
            tc('md5'),
            tc('share'),
            tc('sync_start1'),
            tc('sync_start2'),
            tc('sync_end1'),
            tc('sync_end2'),
            tc('pool_id')
        ]
        fields_sql = ', '.join(fields)

        if size_gte is not None:
            where_list = [f"{tc('fod')} AND {tc('si')} >= {size_gte}"]
            order_by_list = [f'{tc("si")} ASC', f'{tc("id")} ASC']
        else:
            where_list = [f"{tc('fod')} AND {tc('id')} > {id_gt}"]
            order_by_list = [f'{tc("id")} ASC']

        if meet_time is None:
            meet_time = self.get_meet_time()

        meet_time_str = db_datetime_str(meet_time)
        where_list.append(f"({tc('upt')} < '{meet_time_str}' OR {tc('upt')} IS NULL)")

        num_where_items = []
        if BackupNum.ONE in backup_nums:
            num_where_items.append(f"{tc('sync_start1')} IS NULL OR {tc('sync_end1')} IS NULL "
                                   f"OR {tc('upt')} > {tc('sync_start1')}")

        if BackupNum.TWO in backup_nums:
            num_where_items.append(f"{tc('sync_start2')} IS NULL OR {tc('upt')} > {tc('sync_start2')} "
                                   f"OR {tc('sync_end2')} IS NULL")

        if num_where_items:
            num_where = ' OR '.join(num_where_items)
            where_list.append(f"({num_where})")

        if id_mod_div is not None and id_mod_equal is not None:
            if id_mod_div >= 1 and (0 <= id_mod_equal < id_mod_div):
                where_list.append(f"MOD({tc('id')}, {id_mod_div}) = {id_mod_equal}")

        where = " AND ".join(where_list)
        order_by = ', '.join(order_by_list)
        sql = f"SELECT {fields_sql} FROM {qn(table_name)} WHERE ({where}) ORDER BY {order_by} LIMIT {limit}"

        return sql

    def get_need_async_objects(
            self, bucket_id, id_gt: int = None, limit: int = 100, meet_time=None,
            id_mod_div: int = None, id_mod_equal: int = None, backup_nums: list = None,
            size_gte: int = None
    ):
        """
        获取需要同步的对象

            * id_gt和size_gte不能同时使用
            * 默认按id正序排序；
            * 当size_gte有效时，先按object size正序，后按id正序排序；

        :param bucket_id: bucket id
        :param id_gt: 查询id大于id_gt的数据，实现分页续读
        :param limit: 获取数据的数量
        :param meet_time: 查询upt大于此时间的对象
        :param id_mod_div: object id求余的除数，和参数id_mod_equal一起使用
        :param id_mod_equal: object id求余的余数相等的筛选条件，仅在参数id_mod有效时有效
        :param backup_nums: 筛选条件，只查询指定备份点编号需要同步的对象，
        :param size_gte: 查询object size大于等于size_gte的数据
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
            id_mod_equal=id_mod_equal, backup_nums=backup_nums, size_gte=size_gte
        )
        return self.select_all(using=METADATA, sql=sql)

    def get_meet_time(self):
        return datetime.utcnow() - timedelta(minutes=self.MEET_ASYNC_TIMEDELTA_MINUTES)

    def update_object_sync_end_time(self, bucket_id, obj_id, async_time, backup_num):
        """
        备份完成后 sync_end 字段更新
        :param bucket_id:
        :param obj_id:
        :param async_time:
        :param backup_num:
        :return: True or False
        """
        async_time_str = db_datetime_str(async_time)
        table_name = self._bucket_table_name(bucket_id)
        tc = table_columns(table_name=table_name)
        qn = quote_name

        if backup_num == BackupNum.ONE:
            update_field = 'sync_end1'
        else:
            update_field = 'sync_end2'

        fields_set = f"{tc(update_field)} = '{async_time_str}'"
        where = f"{tc('id')} = {obj_id}"
        sql = f"UPDATE {qn(table_name)} SET {fields_set} WHERE {where}"
        try:
            rows = self.update(using=METADATA, sql=sql)
        except Exception as exc:
            rows = self.update(using=METADATA, sql=sql)

        if rows == 1:
            return True

        return False

    def update_sync_start_and_end_before_upload_obj(self, bucket_id, obj_id, async_time, backup_num):
        """
        备份对象前更新数据库sync_start sync_end字段
        :param backup_num:
        :param bucket_id:
        :param obj_id:
        :param async_time:
        :return: True or False
        """
        async_time_str = db_datetime_str(async_time)
        table_name = self._bucket_table_name(bucket_id)
        tc = table_columns(table_name=table_name)
        qn = quote_name

        if backup_num == BackupNum.ONE:
            update_field_end = 'sync_end1'
            update_field_start = 'sync_start1'
        else:
            update_field_end = 'sync_end2'
            update_field_start = 'sync_start2'

        fields_set = f"{tc(update_field_end)} = NULL , {tc(update_field_start)} = '{async_time_str}'"
        where = f"{tc('id')} = {obj_id}"
        sql = f"UPDATE {qn(table_name)} SET {fields_set} WHERE {where}"
        try:
            rows = self.update(using=METADATA, sql=sql)
        except Exception as exc:
            rows = self.update(using=METADATA, sql=sql)

        if rows == 1:
            return True

        return False

    def get_bucket_backup_sql(self, bucket_id, backup_num: int):
        """

        INNER JOIN `buckets_bucket` ON (`bucket_id` = `buckets_bucket`.`id`)
        """
        table_name = 'buckets_backupbucket'
        qn = quote_name
        tc = table_columns(table_name=table_name)

        fields = [
            tc('id'),
            tc('bucket_id'),
            tc('endpoint_url'),
            tc('bucket_token'),
            tc('bucket_name'),
            tc('created_time'),
            tc('modified_time'),
            tc('remarks'),
            tc('status'),
            tc('backup_num'),
            tc('error')
        ]
        fields_sql = ', '.join(fields)

        where = " AND ".join(
            [
                f"{tc('bucket_id')} = {bucket_id}",
                f"{tc('backup_num')} = {backup_num}"
            ]
        )
        sql = f"SELECT {fields_sql} FROM {qn(table_name)} WHERE ({where}) " \
              f"ORDER BY {tc('id')} DESC LIMIT 1"
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

    # ceph 配置 sql语句
    def get_ceph_conf_sql(self):
        """
        获取数据库中ceph配置信息，并动态更新到 settings 文件中。
        """
        table_name = 'ceph_cephcluster'
        tc = table_columns(table_name=table_name)
        qn = quote_name
        fields = [
            tc('id'),
            tc('name'),
            tc('cluster_name'),
            tc('user_name'),
            tc('disable_choice'),
            tc('pool_names'),
            tc('config_file'),
            tc('keyring_file'),
            tc('modified_time'),
            tc('priority_stored_value')
            # tc('alias'),
        ]
        fields_sql = ', '.join(fields)

        sql = f"SELECT {fields_sql} FROM {qn(table_name)}"
        return self.select_all(using=DEFAULT, sql=sql)
