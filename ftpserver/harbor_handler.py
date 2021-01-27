import chardet
import time
from datetime import datetime

from pyftpdlib.handlers import DTPHandler, FileProducer, FTPHandler
from pyftpdlib.filesystems import FilesystemError
from pyftpdlib.log import logger


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
    
    def decode(self, data: bytes):
        """
        这里主要为了解码。
            客户端传送过来的内容需要解码的。解码主要是文件信息，比如文件名，不是指文件内容
        chardet库的使用
            客户端传送过来的数据并不知道是什么编码类型，所以解码也无从下手
            chardet可以帮助识别bytes是什么编码类型，于是可以对症下药，对其进行解码
        问题来源：
            windows默认编码是gbk，而linux以及harbor_ftp服务端默认编码是utf8
            此步主要为了兼容windows的网络映射功能
        """
        # print(chardet.detect(bytes)['encoding'])
        if chardet.detect(data)['encoding'] not in ('utf-8', 'ascii'):
            return data.decode('gbk', self.unicode_errors)
        return data.decode('utf8', self.unicode_errors)

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
            # if isinstance(err, ValueError):
            #     # It could happen if file's last modification time
            #     # happens to be too old (prior to year 1900)
            #     why = "Can't determine file's last modification time"
            # else:
            #     print(err)
            #     why = _strerror(err)
            # self.respond('550 %s.' % why)
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


    # def push(self, s):
    #     asynchat.async_chat.push(self, s.encode('gbk'))

    # def ftp_LIST(self, path):
    #     """Return a list of files in the specified directory to the
    #     client.
    #     On success return the directory path, else None.
    #     """
    #     # - If no argument, fall back on cwd as default.
    #     # - Some older FTP clients erroneously issue /bin/ls-like LIST
    #     #   formats in which case we fall back on cwd as default.
    #     try:
    #         isdir = self.fs.isdir(path)
    #         if isdir:
    #             listing = self.run_as_current_user(self.fs.listdir, path)
    #             if isinstance(listing, list):
    #                 try:
    #                     # RFC 959 recommends the listing to be sorted.
    #                     listing.sort()
    #                 except UnicodeDecodeError:
    #                     # (Python 2 only) might happen on filesystem not
    #                     # supporting gbk meaning os.listdir() returned a list
    #                     # of mixed bytes and unicode strings:
    #                     # http://goo.gl/6DLHD
    #                     # http://bugs.python.org/issue683592
    #                     pass
    #             iterator = self.fs.format_list(path, listing)
    #         else:
    #             basedir, filename = os.path.split(path)
    #             self.fs.lstat(path)  # raise exc in case of problems
    #             iterator = self.fs.format_list(basedir, [filename])
    #     except (OSError, FilesystemError) as err:
    #         why = _strerror(err)
    #         self.respond('550 %s.' % why)
    #     else:
    #         producer = BufferedIteratorProducer(iterator)
    #         self.push_dtp_data(producer, isproducer=True, cmd="LIST")
    #         return path

    # def ftp_NLST(self, path):
    #     """Return a list of files in the specified directory in a
    #     compact form to the client.
    #     On success return the directory path, else None.
    #     """
    #     try:
    #         if self.fs.isdir(path):
    #             listing = list(self.run_as_current_user(self.fs.listdir, path))
    #         else:
    #             # if path is a file we just list its name
    #             self.fs.lstat(path)  # raise exc in case of problems
    #             listing = [os.path.basename(path)]
    #     except (OSError, FilesystemError) as err:
    #         self.respond('550 %s.' % _strerror(err))
    #     else:
    #         data = ''
    #         if listing:
    #             try:
    #                 listing.sort()
    #             except UnicodeDecodeError:
    #                 # (Python 2 only) might happen on filesystem not
    #                 # supporting gbk meaning os.listdir() returned a list
    #                 # of mixed bytes and unicode strings:
    #                 # http://goo.gl/6DLHD
    #                 # http://bugs.python.org/issue683592
    #                 ls = []
    #                 for x in listing:
    #                     if not isinstance(x, unicode):
    #                         x = unicode(x, 'gbk')
    #                     ls.append(x)
    #                 listing = sorted(ls)
    #             data = '\r\n'.join(listing) + '\r\n'
    #         data = data.encode('gbk', self.unicode_errors)
    #         self.push_dtp_data(data, cmd="NLST")
    #         return path

    # def ftp_MLST(self, path):
    #     """Return information about a pathname in a machine-processable
    #     form as defined in RFC-3659.
    #     On success return the path just listed, else None.
    #     """
    #     line = self.fs.fs2ftp(path)
    #     basedir, basename = os.path.split(path)
    #     perms = self.authorizer.get_perms(self.username)
    #     try:
    #         iterator = self.run_as_current_user(
    #             self.fs.format_mlsx, basedir, [basename], perms,
    #             self._current_facts, ignore_err=False)
    #         data = b''.join(iterator)
    #     except (OSError, FilesystemError) as err:
    #         self.respond('550 %s.' % _strerror(err))
    #     else:
    #         data = data.decode('gbk', self.unicode_errors)
    #         # since TVFS is supported (see RFC-3659 chapter 6), a fully
    #         # qualified pathname should be returned
    #         data = data.split(' ')[0] + ' %s\r\n' % line
    #         # response is expected on the command channel
    #         self.push('250-Listing "%s":\r\n' % line)
    #         # the fact set must be preceded by a space
    #         self.push(' ' + data)
    #         self.respond('250 End MLST.')
    #         return path

    # def handle_auth_success(self, home, password, msg_login):
    #     if not isinstance(home, unicode):
    #         if PY3:
    #             raise TypeError('type(home) != text')
    #         else:
    #             warnings.warn(
    #                 '%s.get_home_dir returned a non-unicode string; now '
    #                 'casting to unicode' % (
    #                     self.authorizer.__class__.__name__),
    #                 RuntimeWarning)
    #             home = home.decode('gbk')

    #     if len(msg_login) <= 75:
    #         self.respond('230 %s' % msg_login)
    #     else:
    #         self.push("230-%s\r\n" % msg_login)
    #         self.respond("230 ")
    #     self.log("USER '%s' logged in." % self.username)
    #     self.authenticated = True
    #     self.password = password
    #     self.attempted_logins = 0

    #     self.fs = self.abstracted_fs(home, self)
    #     self.on_login(self.username)

    # def ftp_STAT(self, path):
    #     """Return statistics about current ftp session. If an argument
    #     is provided return directory listing over command channel.

    #     Implementation note:

    #     RFC-959 does not explicitly mention globbing but many FTP
    #     servers do support it as a measure of convenience for FTP
    #     clients and users.

    #     In order to search for and match the given globbing expression,
    #     the code has to search (possibly) many directories, examine
    #     each contained filename, and build a list of matching files in
    #     memory.  Since this operation can be quite intensive, both CPU-
    #     and memory-wise, we do not support globbing.
    #     """
    #     # return STATus information about ftpd
    #     if not path:
    #         s = []
    #         s.append('Connected to: %s:%s' % self.socket.getsockname()[:2])
    #         if self.authenticated:
    #             s.append('Logged in as: %s' % self.username)
    #         else:
    #             if not self.username:
    #                 s.append("Waiting for username.")
    #             else:
    #                 s.append("Waiting for password.")
    #         if self._current_type == 'a':
    #             type = 'ASCII'
    #         else:
    #             type = 'Binary'
    #         s.append("TYPE: %s; STRUcture: File; MODE: Stream" % type)
    #         if self._dtp_acceptor is not None:
    #             s.append('Passive data channel waiting for connection.')
    #         elif self.data_channel is not None:
    #             bytes_sent = self.data_channel.tot_bytes_sent
    #             bytes_recv = self.data_channel.tot_bytes_received
    #             elapsed_time = self.data_channel.get_elapsed_time()
    #             s.append('Data connection open:')
    #             s.append('Total bytes sent: %s' % bytes_sent)
    #             s.append('Total bytes received: %s' % bytes_recv)
    #             s.append('Transfer elapsed time: %s secs' % elapsed_time)
    #         else:
    #             s.append('Data connection closed.')

    #         self.push('211-FTP server status:\r\n')
    #         self.push(''.join([' %s\r\n' % item for item in s]))
    #         self.respond('211 End of status.')
    #     # return directory LISTing over the command channel
    #     else:
    #         line = self.fs.fs2ftp(path)
    #         try:
    #             isdir = self.fs.isdir(path)
    #             if isdir:
    #                 listing = self.run_as_current_user(self.fs.listdir, path)
    #                 if isinstance(listing, list):
    #                     try:
    #                         # RFC 959 recommends the listing to be sorted.
    #                         listing.sort()
    #                     except UnicodeDecodeError:
    #                         # (Python 2 only) might happen on filesystem not
    #                         # supporting gbk meaning os.listdir() returned a
    #                         # list of mixed bytes and unicode strings:
    #                         # http://goo.gl/6DLHD
    #                         # http://bugs.python.org/issue683592
    #                         pass
    #                 iterator = self.fs.format_list(path, listing)
    #             else:
    #                 basedir, filename = os.path.split(path)
    #                 self.fs.lstat(path)  # raise exc in case of problems
    #                 iterator = self.fs.format_list(basedir, [filename])
    #         except (OSError, FilesystemError) as err:
    #             why = _strerror(err)
    #             self.respond('550 %s.' % why)
    #         else:
    #             self.push('213-Status of "%s":\r\n' % line)
    #             self.push_with_producer(BufferedIteratorProducer(iterator))
    #             self.respond('213 End of status.')
    #             return path

    # def ftp_FEAT(self, line):
        # """List all new features supported as defined in RFC-2398."""
        # features = set(['gbk', 'TVFS'])
        # features.update([feat for feat in
        #                  ('EPRT', 'EPSV', 'MDTM', 'MFMT', 'SIZE')
        #                  if feat in self.proto_cmds])
        # features.update(self._extra_feats)
        # if 'MLST' in self.proto_cmds or 'MLSD' in self.proto_cmds:
        #     facts = ''
        #     for fact in self._available_facts:
        #         if fact in self._current_facts:
        #             facts += fact + '*;'
        #         else:
        #             facts += fact + ';'
        #     features.add('MLST ' + facts)
        # if 'REST' in self.proto_cmds:
        #     features.add('REST STREAM')
        # features = sorted(features)
        # self.push("211-Features supported:\r\n")
        # self.push("".join([" %s\r\n" % x for x in features]))
        # self.respond('211 End FEAT.')