import logging

import concurrent_log_handler
from pyftpdlib.servers import (
    # FTPServer,
    # ThreadedFTPServer,
    MultiprocessFTPServer
)
from pyftpdlib.log import logger

from harbor_file_system import HarborFileSystem
from harbor_auth import HarborAuthorizer
from harbor_handler import HarborDTPHandler, HarborFTPHandler, work_mode_in_tls


def main():
    
    # Instantiate a dummy authorizer for managing 'virtual' users
    authorizer = HarborAuthorizer()
 
    # Instantiate FTP handler class
    handler = HarborFTPHandler

    handler.abstracted_fs = HarborFileSystem
    handler.dtp_handler = HarborDTPHandler
    handler.authorizer = authorizer
    if work_mode_in_tls:
        print('work mode in TLS')
        handler.certfile = '/etc/nginx/conf.d/ftp-keycert.pem'
        handler.tls_data_required = True

    logging.basicConfig(level=logging.INFO)
    
    file_handler = concurrent_log_handler.ConcurrentRotatingFileHandler(filename='/var/log/iharbor/ftp.log',
                                                                        maxBytes=1024 ** 2 * 128,
                                                                        backupCount=10, use_gzip=True)
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    # Define a customized banner (string returned when client connects)
    handler.banner = "pyftpdlib based ftpd ready."
 
    # Specify a masquerade address and the range of ports to use for
    # passive connections.  Decomment in case you're behind a NAT.
    # handler.masquerade_address = '151.25.42.11'
    handler.passive_ports = range(2000, 3001)

    # server = FTPServer(address, handler)
    # server = ThreadedFTPServer(address, handler)
    while True:
        try:
            init_server_and_run(handler)
        except (KeyboardInterrupt, SystemExit):
            break
        except Exception:
            continue
        else:       # 正常退出
            break


def init_server_and_run(handler):
    address = ('0.0.0.0', 21)
    server = MultiprocessFTPServer(address, handler)
    # set a limit for connections
    server.max_cons = 2048
    server.max_cons_per_ip = 2048

    # start ftp server
    server.serve_forever()
    server.close_all()

 
if __name__ == '__main__':
    main()
