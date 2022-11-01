#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2022
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

import time
import unittest

from ngamsLib.utils import Task


class TestTask(unittest.TestCase):

    flag = False

    def _set_flag(self, stop_evt):
        if stop_evt.is_set():
            return
        self.flag = True

    def test_task_restart(self):
        """
        Check that a Task can be restarted successfuly and its logic runs to
        completion each time.
        """
        task = Task('test_task', self._set_flag)

        # A task should be able to be restarted and run successfuly every time
        for _ in range(10):
            self.flag = False
            task.start()
            start = time.time()
            timeout = 5
            while not self.flag and time.time() - start < timeout:
                time.sleep(0.001)
            self.assertEqual(True, self.flag)
            task.stop()