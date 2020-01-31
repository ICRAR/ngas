#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2020
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
from test import ngamsTestLib
from ngamsServer import request_db


class DummyRequest(object):
    '''Dummy request type stored during these tests'''

    def __init__(self, request_id, data=None):
        self.request_id = request_id
        self.data = data or request_id

    def getRequestId(self):
        return self.request_id

    def clone(self):
        return DummyRequest(self.request_id, self.data)

    def __eq__(self, other):
        return (other.request_id == self.request_id and
                other.data == self.data)

class RequestDbTestBase(object):

    def test_full_cycle(self):
        db = self.create_request_db()
        self.assertEqual([], list(db.keys()))
        self.assertIsNone(db.get('0'))
        db.delete(['0'])

        req0, req1, req2 = (DummyRequest(str(i)) for i in range(3))

        db.add(req0)
        self.assertEqual(req0, db.get('0'))
        self.assertEqual('0', db.get('0').data)
        self.assertIsNone(db.get('1'))
        self.assertEqual(['0'], list(db.keys()))
        for k in db.keys():
            self.assertIsNotNone(db.get(k))

        req0.data = 'a'
        db.update(req0)
        for k in db.keys():
            self.assertIsNotNone(db.get(k))
        self.assertEqual(req0, db.get('0'))
        self.assertEqual('a', db.get('0').data)
        self.assertIsNone(db.get('1'))
        self.assertEqual(['0'], list(db.keys()))

        db.delete(['0'])
        self.assertEqual([], list(db.keys()))
        self.assertIsNone(db.get('0'))

        db.add(req0)
        db.add(req1)
        db.add(req2)
        for k in db.keys():
            self.assertIsNotNone(db.get(k))
        self.assertEqual(['0', '1', '2'], sorted(list(db.keys())))

class TestDBMRequestsDB(RequestDbTestBase, ngamsTestLib.ngamsTestSuite):
    def create_request_db(self):
        return request_db.DBMRequestDB(ngamsTestLib.genTmpFilename())

class TestInMemoryRequestDB(RequestDbTestBase, ngamsTestLib.ngamsTestSuite):
    def create_request_db(self):
        return request_db.InMemoryRequestDB()

class TestNullRequestDB(ngamsTestLib.ngamsTestSuite):

    def test_full_cycle_runs(self):
        db = request_db.NullRequestDB()
        db.add()
        db.update()
        db.delete()
        db.get()
        db.keys()