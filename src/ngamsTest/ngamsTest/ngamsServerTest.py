#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#
#******************************************************************************
#
# "@(#) $Id: ngamsServerTest.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  18/11/2003  Created
#
"""
This module contains the Test Suite for the NG/AMS Server.
"""

import contextlib
import os
import socket
import subprocess
import sys
import threading
import time
import unittest
import uuid

from ngamsLib import ngamsHttpUtils
from ngamsLib.ngamsCore import NGAMS_SUCCESS, NGAMS_HTTP_SERVICE_NA
from .ngamsTestLib import ngamsTestSuite, saveInFile, sendPclCmd, this_dir


# This module is used as a command by one of its own tests,
# which expects that after running the command there will be a 0-bytes file
# created with a given name
def handleCmd(_, req, __):
    open(os.path.join(this_dir, req['fname']), 'wb').close()


class ngamsServerTest(ngamsTestSuite):

    def test_slow_receiving_client(self):
        """
        This test checks that the NGAS server doesn't hang forever on a slow
        client, since it would block the server for ever
        """

        timeout = 3
        amount_of_data = 10*1024*1024 # 10 MBs
        spaces = " " * amount_of_data
        self.prepExtSrv(cfgProps=[["NgamsCfg.Server[1].TimeOut",str(timeout)]])

        client = sendPclCmd()
        status = client.archive_data(spaces, 'some-file.data', 'application/octet-stream')
        self.assertEquals(NGAMS_SUCCESS, status.getStatus())

        # Normal retrieval works fine
        self.assertEquals(NGAMS_SUCCESS, client.retrieve(fileId='some-file.data').getStatus())
        os.unlink('some-file.data')

        # Now retrieve the data, but sloooooooooooowly and check that the server
        # times out and closes the connection, which in turn makes our receiving
        # end finish earlier than expected. This is detected on the client side
        # because we receive less data than we ask for).
        #
        # We have to make sure that the receiving buffer is tiny so the server
        # really can't write any more data into the socket. In the same spirit
        # we specify a very small send buffer for the server. We don't need to
        # specify a timeout because the recv will return immediately if the
        # server has closed the connection.
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 256)
        s.connect(('localhost', 8888))
        s.send(b'GET /RETRIEVE?file_id=some-file.data&send_buffer=1024 HTTP/1.0\r\n')
        s.send(b'\r\n')
        time.sleep(timeout + 2) # More than enough to provoke a server timeout

        data = s.recv(amount_of_data, socket.MSG_WAITALL)
        self.assertLess(len(data), amount_of_data, "Should have read less data")
        self.assertEquals(b'', s.recv(amount_of_data - len(data)))
        s.close()

    def test_too_many_requests(self):

        saveInFile("tmp/handleHttpRequest_tmp", "handleHttpRequest_Block5secs")
        self.prepExtSrv(srvModule="support.ngamsSrvTestDynReqCallBack",
                        cfgProps=(('NgamsCfg.Server[1].MaxSimReqs', '2'),))

        # Fire off two clients, each takes 5 seconds to finish
        cl1, cl2 =  sendPclCmd(), sendPclCmd()
        threading.Thread(target=cl1.online).start()
        threading.Thread(target=cl2.online).start()

        # The third one should not pass through
        # (assuming that 2 seconds were enough for the two clients
        # to connect and be busy waiting for their reply)
        time.sleep(2)
        resp = ngamsHttpUtils.httpGet('127.0.0.1', 8888, 'ONLINE')
        with contextlib.closing(resp):
            self.assertEqual(NGAMS_HTTP_SERVICE_NA, resp.status)

    def test_reload_command(self):
        """Checks that commands can be reloaded successfully"""
        self.prepExtSrv()
        client = sendPclCmd()
        self.assert_ngas_status(client.status)
        self.assert_ngas_status(client.status, pars=[('reload', '1')])

    def test_user_command_plugin(self):

        # Let this module implement the TEST command
        cfg = (('NgamsCfg.Commands[1].Command[1].Name', 'TEST'),
               ('NgamsCfg.Commands[1].Command[1].Module', 'ngamsTest.ngamsServerTest'))
        self.prepExtSrv(cfgProps=cfg)
        client = sendPclCmd()

        # Let the TEST command create a file under ./tmp
        # There is no need to manually remove here, as ./tmp gets removed anyway
        # later during tearDown()
        fname = os.path.join(this_dir, 'tmp', str(uuid.uuid4()))
        status = client.get_status('TEST', pars=[('fname', fname)])
        self.assertEquals(NGAMS_SUCCESS, status.getStatus())
        self.assertTrue(os.path.isfile(os.path.join(this_dir, fname)))

    def test_no_such_command(self):
        self.prepExtSrv()
        resp, _, _ = sendPclCmd()._get('UNKNOWN_CMD')
        self.assertEquals(404, resp.status)

    @unittest.skipUnless('NGAS_MANY_STARTS_TEST' in os.environ, 'skipped by default')
    def test_many_starts(self):
        for _ in range(int(os.environ['NGAS_MANY_STARTS_TEST'])):
            self.prepExtSrv()
            self.terminateAllServer()

class ngamsDaemonTest(ngamsTestSuite):

    def _run_daemon_cmd(self, cfg_file, cmd):
        execCmd  = [sys.executable, '-m', 'ngamsServer.ngamsDaemon', cmd]
        execCmd += ['-cfg', cfg_file]
        with self._proc_startup_lock:
            daemon_status_proc = subprocess.Popen(execCmd, shell=False)
        return daemon_status_proc.wait()

    def _run_daemon_status(self, cfg_file):
        return self._run_daemon_cmd(cfg_file, 'status')

    def _run_daemon_start(self, cfg_file):
        return self._run_daemon_cmd(cfg_file, 'start')

    def test_start_via_daemon(self):
        self.prepExtSrv(daemon=True)

    def test_daemon_status(self):
        self.prepExtSrv(daemon=True)
        self.assertEquals(0, self._run_daemon_status(self.extSrvInfo[-1].cfg_file))

    def test_daemon_status_no_server_running(self):
        self.assertEquals(1, self._run_daemon_status(os.path.join(this_dir, 'src/ngamsCfg.xml')))

    def test_daemon_double_start(self):
        # Try to start the daemon twice, it should fail
        self.prepExtSrv(daemon=True)
        self.assertNotEqual(0, self._run_daemon_start(os.path.join(this_dir, 'src/ngamsCfg.xml')))