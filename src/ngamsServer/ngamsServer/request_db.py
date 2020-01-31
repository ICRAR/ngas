#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
"""Classes implementing the request DB"""

import errno
import os
import threading

from ngamsLib import ngamsDbm, utils


class NullRequestDB(object):
    """A RequestDB class that implements null behaviour"""

    def noop(self, *args):
        pass
    add = noop
    update = noop
    delete = noop
    get = noop

    def keys(self):
        return []

class InMemoryRequestDB(object):
    """A RequestDB class that keeps requests in memory"""

    def __init__(self):
        self.requests = {}

    def add(self, req):
        self.requests[req.getRequestId()] = req

    def update(self, req):
        # no-op. Request object being updated are actually the same
        # objects in memory that were previously added (although they may have
        # been internally modified). Thus we don't need to update our map
        pass

    def delete(self, req_ids):
        for req_id in req_ids:
            try:
                del self.requests[req_id]
            except KeyError:
                pass

    def get(self, req_id):
        return self.requests.get(req_id, None)

    def keys(self):
        return list(self.requests)

class DBMRequestDB(object):
    """A RequestDB backed up by a DBM file"""

    def __init__(self, fname):
        self.dbm_fname = fname
        self.lock = threading.Lock()
        self.recreate_dbm()

    def recreate_dbm(self):
        try:
            os.unlink(self.dbm_fname)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
        self.dbm = ngamsDbm.ngamsDbm(self.dbm_fname, cleanUpOnDestr=0, writePerm=1)

    def add(self, req):
        with self.lock:
            self.dbm.add(req.getRequestId(), req)
            self.dbm.sync()

    def update(self, req):
        req_id = req.getRequestId()
        req = req.clone()
        with self.lock:
            try:
                self.dbm.add(req_id, req)
                self.dbm.sync()
            except ngamsDbm.DbRunRecoveryError:
                self.recreate_dbm()
                raise

    def delete(self, req_ids):
        req_ids = map(str, req_ids)
        with self.lock:
            try:
                for req_id in req_ids:
                    if req_id in self.dbm:
                        self.dbm.rem(req_id)
                self.dbm.sync()
            except ngamsDbm.DbRunRecoveryError:
                self.recreate_dbm()
                raise

    def get(self, req_id):
        try:
            with self.lock:
                if req_id in self.dbm:
                    return self.dbm.get(req_id)
                return None
        except ngamsDbm.DbRunRecoveryError:
            self.recreate_dbm()
            raise

    def keys(self):
        with self.lock:
            return list(map(utils.b2s, self.dbm.keys()))