import argparse
import MySQLdb as Database
from MySQLdb.cursors import DictCursor
from MySQLdb.constants import CLIENT


class CanNotConnection(Exception):
    pass


class SrcDatabaseError(Exception):
    pass


class DestDatabaseError(Exception):
    pass


class DatabaseWrapper:

    def __init__(self, settings_dict):
        self.settings_dict = settings_dict
        self.connection = None
        self.connect()

    def connect(self):
        conn_params = self.get_connection_params()
        self.connection = self.get_new_connection(conn_params)

    def get_connection_params(self):
        kwargs = {
            'charset': 'utf8',
        }
        settings_dict = self.settings_dict
        if settings_dict['USER']:
            kwargs['user'] = settings_dict['USER']
        if settings_dict['NAME']:
            kwargs['database'] = settings_dict['NAME']
        if settings_dict['PASSWORD']:
            kwargs['password'] = settings_dict['PASSWORD']
        if settings_dict['HOST'].startswith('/'):
            kwargs['unix_socket'] = settings_dict['HOST']
        elif settings_dict['HOST']:
            kwargs['host'] = settings_dict['HOST']
        if settings_dict['PORT']:
            kwargs['port'] = int(settings_dict['PORT'])
        # We need the number of potentially affected rows after an
        # "UPDATE", not the number of changed rows.
        kwargs['client_flag'] = CLIENT.FOUND_ROWS
        if 'connect_timeout' not in kwargs:
            kwargs['connect_timeout'] = 5

        return kwargs

    def get_new_connection(self, conn_params):
        conn_params['cursorclass'] = DictCursor
        try:
            connection = Database.connect(**conn_params)
        except Database.OperationalError as e:
            raise CanNotConnection(str(e))

        if connection.encoders.get(bytes) is bytes:
            connection.encoders.pop(bytes)
        return connection

    def is_usable(self):
        try:
            self.connection.ping()
        except Database.Error:
            return False
        else:
            return True

    def close_if_unusable_or_obsolete(self):
        if self.connection is not None:
            if not self.is_usable():
                self.close()
                return

    def close(self):
        if self.connection is not None:
            self.connection.close()

        self.connection = None

    def cursor(self):
        try:
            conn = self.get_connection()
            return conn.cursor()
        except Exception as e:
            raise e

    def get_connection(self):
        if self.connection is None:
            self.connect()

        return self.connection


class DiffBucket:
    def __init__(self, src_bucket_tablename, dest_bucket_tablename):
        self.src_bucket_tablename = src_bucket_tablename
        self.dest_bucket_tablename = dest_bucket_tablename

    @staticmethod
    def select(database, sql: str, result_type: str = 'all'):
        """
        :param result_type: one, all
        :return:
            (dict, )            # when result_type == all
            dict or None        # when result_type == one
        :raises: Exception
        """
        if result_type not in ['one', 'all']:
            raise Exception('invalid value of param result_type')

        try:
            with database.cursor() as cursor:
                cursor.execute(sql)
                if result_type == 'one':
                    ret = cursor.fetchone()
                elif result_type == 'all':
                    ret = cursor.fetchall()

                return ret
        except Exception as e:
            raise e

    def get_src_objects(self, id_gt: int = 0, limit: int = 1000):
        sql = f'SELECT * FROM `{self.src_bucket_tablename}` WHERE `fod`=1 AND `id` > {id_gt} ' \
              f'ORDER BY `id` ASC LIMIT {limit}'
        return self.select(src_datbase, sql=sql, result_type='all')

    def get_object_from_dest(self, key: str, key_md5: str):
        where_items = [
            f'`na`="{key}"',
            '`fod`=1',
        ]
        if key_md5:
            where_items.insert(0, f'`na_md5`="{key_md5}"')

        where = ' AND '.join(where_items)
        sql = f'SELECT * FROM `{self.dest_bucket_tablename}` WHERE {where}'
        return self.select(dest_datbase, sql=sql, result_type='one')

    def dest_object_exists(self, key: str, key_md5: str):
        obj = self.get_object_from_dest(key=key, key_md5=key_md5)
        if obj is None:
            return False

        return True

    def diff_objects(self, last_id: int, objects):
        """
        :return:
            last_id, lines, ok
        """
        diff_lines = []
        try:
            for obj in objects:
                obj_id = obj['id']
                key = obj['na']
                key_md5 = obj['na_md5']
                if not self.dest_object_exists(key=key, key_md5=key_md5):
                    msg = f'object(id={obj["id"]} ,key="{key}, async1={str(obj["async1"])}")\n'
                    diff_lines.append(msg)
                    print(msg)

                last_id = obj_id
        except Exception as e:
            return last_id, diff_lines, False

        return last_id, diff_lines, True

    def diff(self, file):
        err_count = 0
        last_id = 0
        diff_lines = []
        while True:
            print(f'last_id: {last_id}')
            try:
                objects = self.get_src_objects(id_gt=last_id)
                if len(objects) == 0:
                    break

                last_id, _lines, ok = self.diff_objects(last_id=last_id, objects=objects)
                if not ok:
                    raise Exception('diff_objects error')

                if _lines:
                    diff_lines += _lines

                err_count = max(err_count - 1, 0)
            except Exception as e:
                print(f'error, {str(e)}')
                if isinstance(e, KeyboardInterrupt):
                    err_count += 5
                else:
                    err_count += 1
                if err_count > 10:
                    break
                src_datbase.close_if_unusable_or_obsolete()
                dest_datbase.close_if_unusable_or_obsolete()

            if len(diff_lines) > 1000:
                file.writelines(diff_lines)
                diff_lines.clear()

        diff_lines.append(f'last_id: {last_id}\n')
        file.writelines(diff_lines)


def main(params):
    src_tablename = params.src_tablename
    dest_tablename = params.dest_tablename
    if 'out_filename' in params:
        out_filename = params.out_filename
    else:
        out_filename = '/home/diff-bucket.txt'

    print(f'src settings: {database_src_settings}')
    print(f'dest settings: {database_dest_settings}')
    print(f'src bucket table: {src_tablename}')
    print(f'dest bucket table: {dest_tablename}')
    print(f'output file: {out_filename}')
    if input('Are you sure you want to do? yes or no: ') != 'yes':
        print('Exit, Cancelled')
        exit(0)

    with open(out_filename, 'a+') as f:
        f.write(f'#### {src_tablename} #\n')
        DiffBucket(
            src_bucket_tablename=src_tablename,
            dest_bucket_tablename=dest_tablename
        ).diff(f)


def params_parser():
    parser = argparse.ArgumentParser(description='比对查找备份桶未同步到目标桶的对象')
    parser.add_argument(
        '-src', '--src-tablename', dest='src_tablename', type=str, default='', required=True,
        help='src bucket table name')
    parser.add_argument(
        '-dest', '--dest-tablename', dest='dest_tablename', type=str, default='', required=True,
        help='dest bucket table name')
    parser.add_argument(
        '-out', '--out-filename', dest='out_filename', nargs='?', required=False, type=str,
        help='The file path name that output some msg to.')

    parser.add_argument(
        '-srcpd', '--src-password', dest='src_password', type=str, default='', required=True,
        help='src database password')

    parser.add_argument(
        '-destpd', '--dest-password', dest='dest_password', type=str, default='', required=True,
        help='dest database password')

    return parser.parse_args()


if __name__ == '__main__':
    database_src_settings = {
        'NAME': 'evbuckets',  # 数据的库名
        'HOST': '10.100.50.230',  # 主机
        'PORT': '4000',  # 数据库使用的端口
        'USER': 'root',  # 数据库用户名
        'PASSWORD': 'xxx',  # 密码
    }

    database_dest_settings = {
        'NAME': 'iharbor-metadata',  # 数据的库名
        'HOST': '10.100.50.218',  # 主机
        'PORT': '4000',  # 数据库使用的端口
        'USER': 'root',  # 数据库用户名
        'PASSWORD': 'xxx',  # 密码
    }
    params = params_parser()
    database_src_settings['PASSWORD'] = params.src_password
    database_dest_settings['PASSWORD'] = params.dest_password

    dest_datbase = DatabaseWrapper(settings_dict=database_dest_settings)
    src_datbase = DatabaseWrapper(settings_dict=database_src_settings)
    try:
        main(params)
    finally:
        dest_datbase.close()
        src_datbase.close()
