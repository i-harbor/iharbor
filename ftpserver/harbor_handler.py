from pyftpdlib.handlers import DTPHandler, FileProducer, FTPHandler

class HarborDTPHandler(DTPHandler):
    # ac_in_buffer_size = 8 * 1024 * 1024 * 5
    # ac_out_buffer_size = 8 * 1024 * 1024 * 5
    ac_in_buffer_size = 256 * 1024
    ac_out_buffer_size = 256 * 1024

class HarborFileProducer(FileProducer):
    buffer_size = 32 * 1024 * 1024

class HarborFTPHandler(FTPHandler):
    def ftp_RETR(self, file):
        """Retrieve the specified file (transfer from the server to the
        client).  On success return the file path else None.
        """
        rest_pos = self._restart_position
        self._restart_position = 0
        try:
            fd = self.run_as_current_user(self.fs.open, file, 'rb')
        except (EnvironmentError, FilesystemError) as err:
            why = _strerror(err)
            self.respond('550 %s.' % why)
            return

        try:
            if rest_pos:
                # Make sure that the requested offset is valid (within the
                # size of the file being resumed).
                # According to RFC-1123 a 554 reply may result in case that
                # the existing file cannot be repositioned as specified in
                # the REST.
                ok = 0
                try:
                    if rest_pos > self.fs.getsize(file):
                        raise ValueError
                    fd.seek(rest_pos)
                    ok = 1
                except ValueError:
                    why = "Invalid REST parameter"
                except (EnvironmentError, FilesystemError) as err:
                    why = _strerror(err)
                if not ok:
                    fd.close()
                    self.respond('554 %s' % why)
                    return
            producer = HarborFileProducer(fd, self._current_type)
            self.push_dtp_data(producer, isproducer=True, file=fd, cmd="RETR")
            return file
        except Exception:
            fd.close()
            raise