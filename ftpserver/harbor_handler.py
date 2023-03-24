import time
from datetime import datetime

from pyftpdlib.filesystems import FilesystemError
from pyftpdlib.log import logger
from pyftpdlib.handlers import FileProducer, SSL

work_mode_in_tls = True
if SSL is None:
    work_mode_in_tls = False

if work_mode_in_tls:
    from pyftpdlib.handlers import (TLS_DTPHandler as DTPHandler, TLS_FTPHandler as FTPHandler)
else:
    from pyftpdlib.handlers import DTPHandler, FTPHandler


class HarborDTPHandler(DTPHandler):
    """
    继承DTPHandler，修改上传数据块的大小
    """
    # ac_in_buffer_size = 8 * 1024 * 1024 * 5
    # ac_out_buffer_size = 8 * 1024 * 1024 * 5
    ac_in_buffer_size = 256 * 1024
    ac_out_buffer_size = 256 * 1024


class HarborFileProducer(FileProducer):
    """
    继承FileProducer，修改下载数据块的大小
    """
    buffer_size = 32 * 1024 * 1024


class HarborFTPHandler(FTPHandler):
    """
    继承FTPHandler，主要为了处理编码问题。
    """
    def ftp_RETR(self, file):
        """Retrieve the specified file (transfer from the server to the
        client).  On success return the file path else None.
        """
        rest_pos = self._restart_position
        self._restart_position = 0
        try:
            fd = self.run_as_current_user(self.fs.open, file, 'rb')
        except (EnvironmentError, FilesystemError) as err:
            # why = _strerror(err)
            why = str(err)
            self.respond('550 %s.' % why)
            return

        try:
            if rest_pos:
                # Make sure that the requested offset is valid (within the
                # size of the file being resumed).
                # According to RFC-1123 a 554 reply may result in case that
                # the existing file cannot be repositioned as specified in
                # the REST.
                try:
                    if rest_pos > self.fs.getsize(file):
                        raise ValueError("Invalid REST parameter")
                    fd.seek(rest_pos)
                except (ValueError, EnvironmentError, FilesystemError) as err:
                    # why = _strerror(err)
                    why = str(err)
                    fd.close()
                    self.respond('554 %s' % why)
                    return

            producer = HarborFileProducer(fd, self._current_type)
            self.push_dtp_data(producer, isproducer=True, file=fd, cmd="RETR")
            return file
        except Exception:
            fd.close()
            raise

    def ftp_MFMT(self, path, timeval):
        """ Sets the last modification time of file to timeval
        3307 style timestamp (YYYYMMDDHHMMSS) as defined in RFC-3659.
        On success return the modified time and file path, else None.
        """
        # Note: the MFMT command is not a formal RFC command
        # but stated in the following MEMO:
        # https://tools.ietf.org/html/draft-somers-ftp-mfxx-04
        # this is implemented to assist with file synchronization

        line = self.fs.fs2ftp(path)
        # print('its me')
        if len(timeval) != len("YYYYMMDDHHMMSS"):
            why = "Invalid time format; expected: YYYYMMDDHHMMSS"
            self.respond('550 %s.' % why)
            return
        if not self.fs.isfile(self.fs.realpath(path)):
            self.respond("550 %s is not retrievable" % line)
            return
        if self.use_gmt_times:
            timefunc = time.gmtime
        else:
            timefunc = time.localtime
        try:
            # convert timeval string to epoch seconds
            epoch = datetime.utcfromtimestamp(0)
            timeval_datetime_obj = datetime.strptime(timeval, '%Y%m%d%H%M%S')
            timeval_secs = (timeval_datetime_obj - epoch).total_seconds()
        except ValueError:
            why = "Invalid time format; expected: YYYYMMDDHHMMSS"
            self.respond('550 %s.' % why)
            return
        try:
            # Modify Time
            self.run_as_current_user(self.fs.utime, path, timeval_secs)
            # Fetch Time
            secs = self.run_as_current_user(self.fs.getmtime, path)
            lmt = time.strftime("%Y%m%d%H%M%S", timefunc(secs))
        except (ValueError, OSError, FilesystemError) as err:
            lmt = timeval
            self.respond("213 Modify=%s; %s." % (lmt, line))
            return lmt, path
        else:
            self.respond("213 Modify=%s; %s." % (lmt, line))
            return lmt, path

    def ftp_SITE_CHMOD(self, path, mode):
        """Change file mode.
        On success return a (file_path, mode) tuple.
        """
        # Note: although most UNIX servers implement it, SITE CHMOD is not
        # defined in any official RFC.
        try:
            assert len(mode) in (3, 4)
            for x in mode:
                assert 0 <= int(x) <= 7
            mode = int(mode, 8)
        except (AssertionError, ValueError):
            self.respond("501 Invalid SITE CHMOD format.")
        else:
            try:
                self.run_as_current_user(self.fs.chmod, path, mode)
            except (OSError, FilesystemError) as err:
                # why = _strerror(err)
                # self.respond('550 %s.' % why)
                self.respond('200 SITE CHMOD successful.')
                return path, mode
            else:
                self.respond('200 SITE CHMOD successful.')
                return path, mode

    def log(self, msg, logfun=logger.info):
        """Log a message, including additional identifying session data."""
        prefix = self.log_prefix % self.__dict__
        # print(self.__dict__)
        _time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        logfun("[%s] %s %s" % (_time, prefix, msg))

    def logline(self, msg, logfun=logger.debug):
        """Log a line including additional indentifying session data.
        By default this is disabled unless logging level == DEBUG.
        """
        if self._log_debug:
            prefix = self.log_prefix % self.__dict__
            _time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            logfun("[%s] %s %s" % (_time, prefix, msg))

    def logerror(self, msg):
        """Log an error including additional indentifying session data."""
        prefix = self.log_prefix % self.__dict__
        _time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        logger.error("[%s] %s %s" % (_time, prefix, msg))
