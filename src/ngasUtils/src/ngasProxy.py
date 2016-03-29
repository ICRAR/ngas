#!/usr/bin/env python
#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2016
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved

# Generate self signed keys
# openssl req -x509 -sha256 -newkey rsa:2048 -keyout key.pem -out cert.pem -nodes

import os
import ssl
import sys
import json
import urllib
import urllib2
import base64
import socket
import logging
import urlparse

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
from contextlib import closing
from ConfigParser import SafeConfigParser

if not os.path.exists('./log'):
   os.makedirs('./log')

logger = logging.getLogger('ngas_proxy')
logger.setLevel(logging.DEBUG)
logger.propagate = False
rot = logging.FileHandler('./log/proxy.log')
rot.setLevel(logging.DEBUG)
rot.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
logger.addHandler(rot)

cfg = SafeConfigParser()
cfg.read('./proxy.cfg')

SRV = cfg.get('proxy', 'ngas_server')
TIMEO = cfg.getint('proxy', 'ngas_conn_timeout')
USER = cfg.get('proxy', 'ngas_user')
PASSWD = cfg.get('proxy', 'ngas_pass')
READ_TIMEO = cfg.getint('proxy', 'ngas_read_timeout')
BUFFSIZE = cfg.getint('proxy', 'ngas_read_buffer')

CMDS = tuple(cfg.get('proxy', 'ngas_commands_allowed').split(','))
PARAMS = tuple(cfg.get('proxy', 'ngas_params_allowed').split(','))

# do not pass through these headers to NGAS
DENY_HEADERS = tuple(cfg.get('proxy', 'ngas_headers_denied').split(','))


def remove_injects(string):
    # remove characters that could cause shell or sql injection problems
    # temporary until NGAS is fixed
    for ch in [';', '|', '&', '>', '<', '>>', '<<', "'"]:
        if ch in string:
            string = string.replace(ch, '')
    return string

class Handler(BaseHTTPRequestHandler):

    def do_GET(self):

        try:
            addr = self.client_address
            path = self.path
            error_code = 500
            resp_sent = False

            logger.info('Client: %s Incoming Request. Command: %s Path: %s' %\
                        (addr[0], self.command, path))

            uparse = urlparse.urlparse(path)
            if not uparse.path:
                error_code = 500
                return

            if not uparse.path.lower().startswith(CMDS):
                logger.warning('Client: %s Invalid Command. Path: %s' % (addr[0], path))
                error_code = 403
                return

            params = urlparse.parse_qs(uparse.query)
            for key, val in params.items():
                if key.lower() not in PARAMS:
                    logger.warning('Client: %s Invalid Params. Path: %s' % (addr[0], path))
                    error_code = 403
                    return

            head = {}
            for h in self.headers:
                if h.lower() in DENY_HEADERS:
                    logger.warning('Client: %s Header Denied. Headers: %s Path: %s' %\
                                (addr[0], h, path))
                    continue
                head[h] = self.headers.get(h)

            url = 'http://%s%s' % (SRV, path)
            req = urllib2.Request(url, headers = head)
            enc = base64.b64encode(('%s:%s' % (USER, PASSWD)).encode('ascii'))
            req.add_header('Authorization', 'Basic %s' % enc.decode('ascii'))

            with closing(urllib2.urlopen(req, timeout = TIMEO)) as conn:
                self.send_response(conn.getcode())
                headers = dict(conn.headers)
                for key, value in headers.iteritems():
                    self.send_header(key, value)
                self.end_headers()

                resp_sent = True
                length = int(headers.get('content-length', 0))
                if length <= 0:
                    return

                logger.info('Client: %s Path: %s Content Length: %s' %\
                            (addr[0], path, length))

                readin = 0
                while readin < length:
                    try:
                        # must put a try except here as the socket
                        # can be set to None on error
                        conn.fp._sock.fp._sock.settimeout(READ_TIMEO)
                    except:
                        pass
                    left = length - readin
                    buff = conn.read(BUFFSIZE if left >= BUFFSIZE else left)
                    if not buff:
                        raise IOError('socket read error')
                    self.wfile.write(buff)
                    readin += len(buff)

        except socket.timeout as s:
            logger.error('Client: %s Socket timeout. Path: %s' % (addr[0], path))
            error_code = 504
        except IOError as i:
            logger.error('Client: %s Socket IO Error. Path: %s' % (addr[0], path))
            error_code = 503
        except Exception as e:
            logger.error('Client: %s General Exception. Error: %s Path: %s' % \
                        (addr[0], str(e), path))
            error_code = 500
        finally:
            logger.info('Client: %s Request Complete. Path: %s' % (addr[0], path))
            try:
                if not resp_sent:
                    self.send_response(error_code)
                    self.end_headers()
            except:
                pass

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass

if __name__ == '__main__':
    try:
        httpd = ThreadedHTTPServer(('', cfg.getint('proxy', 'port')), Handler)
        httpd.daemon_threads = True
        httpd.socket = ssl.wrap_socket(httpd.socket,
                                        server_side = True,
                                        cert_reqs = ssl.CERT_REQUIRED,
                                        certfile = cfg.get('proxy', 'cert'),
                                        keyfile = cfg.get('proxy', 'key'),
                                        ca_certs = cfg.get('proxy', 'root_ca'))
        logger.info('Proxy started')
        httpd.serve_forever()
    except KeyboardInterrupt as k:
        pass
