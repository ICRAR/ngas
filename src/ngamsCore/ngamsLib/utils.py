#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2018
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

import logging
import multiprocessing
import sys
import threading
import time
import os
import signal

logger = logging.getLogger(__name__)

if sys.version_info[0] > 2:
    def b2s(b, enc='utf8'):
        return b.decode(enc)
    _long = int
else:
    def b2s(b, _='utf8'):
        return b
    _long = long

class Task(object):
    """
    A task that runs on the background. The task (a function) can be executed
    either on a separate thread (default) or in a separate process depending
    on the `mode` argument given at construction time.

    To enable interruption, at `start` time the target functions are called with
    an extra parameter in *args containing an event object, which is set at
    `stop` time. This event object therefore allows target functions know when
    they should stop working. This stop event is stored in its `stop_evt` member,
    and is generated automatically by this class unless the `stop_evt` argument
    is specified at construction time. This should allow the creation of multiple
    Task objects sharing a single stopping event.
    """

    THREAD = 0
    PROCESS = 1

    def __init__(self, name, target, mode=THREAD, stop_evt=None):
        """Creates a new task with a name and a target function"""
        self.name = name
        self._target = target
        self._mode = mode
        self.stop_evt = stop_evt or (threading.Event() if mode == Task.THREAD else multiprocessing.Event())
        self._bg_task = None

    def start(self, *args, **kwargs):
        """Starts the target in the background"""
        bg_task_clazz = threading.Thread if self._mode == Task.THREAD else multiprocessing.Process
        args = tuple(args) + (self.stop_evt,)
        self._bg_task = bg_task_clazz(target=self._target, name=self.name,
                                      args=args, kwargs=kwargs)
        self._bg_task.start()

    def stop(self, timeout=10):
        """Interrupts the task and waits until its completion"""
        if self._bg_task is None:
            return
        logger.info("Stopping the %s task", self.name)
        start = time.time()
        self._stop(timeout)
        self._bg_task = None
        logger.info("%s task stopped in %.3f [s]", self.name, (time.time() - start))

    def _stop(self, timeout=10):

        if self._mode == Task.PROCESS:
            code = self._bg_task.exitcode
            if code is not None:
                logger.warning("%s task already exited with code %d", self.name, code)

        if self.stop_evt.is_set():
            return
        self.stop_evt.set()
        self._bg_task.join(timeout)

        if self._mode == Task.PROCESS:
            code = self._bg_task.exitcode
            if code is None:
                logger.warning("%s task didn't exit cleanly (exit code=%d), killing it". self.name, code)
                os.kill(self._bg_task.pid, signal.SIGKILL)
        elif self._bg_task.is_alive():
            logger.warning("Task %s is still alive after stopping it, continuing anyway", self.name)