import threading
import MySQLdb as Database
from MySQLdb.constants import CLIENT
from MySQLdb.cursors import DictCursor

from webserver import settings as django_settings

backup_setting = getattr(django_settings, 'BACKUP_BUCKET_SETTINGS', {})
meet_async_timedelta_minutes = backup_setting.get('meet_async_timedelta_minutes', 60)


class ConnectionDoesNotExist(Exception):
    pass


class ConnectionHandler:
    settings_name = 'DATABASES'
    exception_class = ConnectionDoesNotExist

    def __init__(self, settings: dict = None):
        if settings is None:
            settings = getattr(django_settings, self.settings_name)

        self._settings = settings
        self._connections = {}

    @property
    def settings(self) -> dict:
        return self._settings

    def __getitem__(self, alias):
        try:
            return self._connections[alias]
        except KeyError:
            if alias not in self.settings:
                raise self.exception_class(f"The connection '{alias}' doesn't exist.")
        conn = self.create_connection(alias)
        self._connections[alias] = conn
        return conn

    def __setitem__(self, key, value):
        self._connections[key] = value

    def __delitem__(self, key):
        self._connections.pop(key, None)

    def __iter__(self):
        return iter(self.settings.keys())

    def all(self):
        return [self[alias] for alias in self]

    def create_connection(self, alias):
        self.ensure_defaults(alias)
        db = self.settings[alias]
        return DatabaseWrapper(db, alias)

    def close_all(self):
        for alias in self:
            try:
                connection = getattr(self._connections, alias)
            except AttributeError:
                continue
            connection.close()

    def ensure_defaults(self, alias):
        """
        Put the defaults into the settings dictionary for a given connection
        where no settings is provided.
        """
        try:
            conn = self.settings[alias]
        except KeyError:
            raise self.exception_class(f"The connection '{alias}' doesn't exist.")

        conn.setdefault('CONN_MAX_AGE', 0)
        conn.setdefault('OPTIONS', {})
        conn.setdefault('TIME_ZONE', None)
        for setting in ['NAME', 'USER', 'PASSWORD', 'HOST', 'PORT']:
            conn.setdefault(setting, '')


class DatabaseWrapper:
    isolation_levels = {
        'read uncommitted',
        'read committed',
        'repeatable read',
        'serializable',
    }

    def __init__(self, settings_dict, alias):
        self.settings_dict = settings_dict
        self.alias = alias
        self.connection = None
        self.connect()
        self.errors_occurred = False

    def connect(self):
        # Reset parameters defining when to close the connection
        # max_age = self.settings_dict['CONN_MAX_AGE']
        # self.close_at = None if max_age is None else time.monotonic() + max_age
        # self.errors_occurred = False
        # Establish the connection
        conn_params = self.get_connection_params()
        self.connection = self.get_new_connection(conn_params)
        self.init_connection_state()

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
        # Validate the transaction isolation level, if specified.
        options = settings_dict['OPTIONS'].copy()
        isolation_level = options.pop('isolation_level', 'read committed')
        if isolation_level:
            isolation_level = isolation_level.lower()
            if isolation_level not in self.isolation_levels:
                raise Exception(
                    "Invalid transaction isolation level '%s' specified.\n"
                    "Use one of %s, or None." % (
                        isolation_level,
                        ', '.join("'%s'" % s for s in sorted(self.isolation_levels))
                    ))
        self.isolation_level = isolation_level
        kwargs.update(options)
        return kwargs

    def get_new_connection(self, conn_params):
        conn_params['cursorclass'] = DictCursor
        connection = Database.connect(**conn_params)
        # bytes encoder in mysqlclient doesn't work and was added only to
        # prevent KeyErrors in Django < 2.0. We can remove this workaround when
        # mysqlclient 2.1 becomes the minimal mysqlclient supported by Django.
        # See https://github.com/PyMySQL/mysqlclient/issues/489
        if connection.encoders.get(bytes) is bytes:
            connection.encoders.pop(bytes)
        return connection

    def init_connection_state(self):
        assignments = []
        if self.isolation_level:
            assignments.append('SET SESSION TRANSACTION ISOLATION LEVEL %s' % self.isolation_level.upper())

        if assignments:
            with self.connection.cursor() as cursor:
                cursor.execute('; '.join(assignments))

    def is_usable(self):
        try:
            self.connection.ping()
        except Database.Error:
            return False
        else:
            return True

    def close_if_unusable_or_obsolete(self):
        """
        Close the current connection if unrecoverable errors have occurred
        or if it outlived its maximum age.
        """
        if self.connection is not None:
            # If an exception other than DataError or IntegrityError occurred
            # since the last commit / rollback, check if the connection works.
            if self.errors_occurred:
                if self.is_usable():
                    self.errors_occurred = False
                else:
                    self.close()
                    return

            # if self.close_at is not None and time.monotonic() >= self.close_at:
            #     self.close()
            #     return

    def mysql_server_data(self):
        conn = self.get_connection()
        with conn.cursor() as cursor:
            # Select some server variables and test if the time zone
            # definitions are installed. CONVERT_TZ returns NULL if 'UTC'
            # timezone isn't loaded into the mysql.time_zone table.
            cursor.execute("""
                SELECT VERSION(),
                       @@sql_mode,
                       @@default_storage_engine,
                       @@sql_auto_is_null,
                       @@lower_case_table_names,
                       CONVERT_TZ('2001-01-01 01:00:00', 'UTC', 'UTC') IS NOT NULL
            """)
            row = cursor.fetchone()
        return {
            'version': row[0],
            'sql_mode': row[1],
            'default_storage_engine': row[2],
            'sql_auto_is_null': bool(row[3]),
            'lower_case_table_names': bool(row[4]),
            'has_zoneinfo_database': bool(row[5]),
        }

    def cursor(self):
        try:
            conn = self.get_connection()
            return conn.cursor()
        except Exception as e:
            self.errors_occurred = True
            raise e

    def commit(self):
        r = self.connection.commit()
        self.errors_occurred = False
        return r

    def close(self):
        if self.connection is not None:
            self.connection.close()

        self.connection = None

    def get_connection(self):
        if self.connection is None:
            self.connect()

        return self.connection


connections = ConnectionHandler()


def close_old_connections(**kwargs):
    for conn in connections.all():
        conn.close_if_unusable_or_obsolete()


def get_connection(using: str):
    close_old_connections()
    return connections[using]
