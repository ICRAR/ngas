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

# Original code is available here:
# https://code.google.com/p/billiejoex/source/browse/works/socket_sendfile.py
#
# Author: Giampaolo Rodola' <g.rodola [AT] gmail [DOT] com>
# License: MIT

import errno
import io
import os
import select
import socket
import sys

# sendfile is provided via os.sendfile in python3.3+, or via the pysendfile
# distribution, but only in posix systems
os_sendfile = None
if os.name == 'posix':
    if sys.version_info[0] == 3:
        os_sendfile = os.sendfile
    else:
        try:
            from sendfile import sendfile as os_sendfile  # @UnresolvedImport
        except ImportError:
            pass


_RETRY = frozenset((errno.EAGAIN, errno.EALREADY, errno.EWOULDBLOCK,
                    errno.EINPROGRESS))


class _GiveupOnSendfile(Exception):
    pass


if os_sendfile is not None:

    def _sendfile_use_sendfile(sock, file, offset=0, count=None):
        _check_sendfile_params(sock, file, offset, count)
        sockno = sock.fileno()
        try:
            fileno = file.fileno()
        except (AttributeError, io.UnsupportedOperation) as err:
            raise _GiveupOnSendfile(err)  # not a regular file
        try:
            fsize = os.fstat(fileno).st_size
        except OSError:
            raise _GiveupOnSendfile(err)  # not a regular file
        if not fsize:
            return 0  # empty file
        blocksize = fsize if not count else count

        timeout = sock.gettimeout()
        if timeout == 0:
            raise ValueError("non-blocking sockets are not supported")
        # poll/select have the advantage of not requiring any
        # extra file descriptor, contrarily to epoll/kqueue
        # (also, they require a single syscall).
        if hasattr(select, 'poll'):
            if timeout is not None:
                timeout *= 1000
            pollster = select.poll()
            pollster.register(sockno, select.POLLOUT)

            def wait_for_fd():
                if pollster.poll(timeout) == []:
                    raise socket._socket.timeout('timed out')
        else:
            # call select() once in order to solicit ValueError in
            # case we run out of fds
            try:
                select.select([], [sockno], [], 0)
            except ValueError:
                raise _GiveupOnSendfile(err)

            def wait_for_fd():
                fds = select.select([], [sockno], [], timeout)
                if fds == ([], [], []):
                    raise socket._socket.timeout('timed out')

        total_sent = 0
        try:
            while True:
                if timeout:
                    wait_for_fd()
                if count:
                    blocksize = count - total_sent
                    if blocksize <= 0:
                        break
                try:
                    sent = os_sendfile(sockno, fileno, offset, blocksize)
                except OSError as err:
                    if err.errno in _RETRY:
                        # Block until the socket is ready to send some
                        # data; avoids hogging CPU resources.
                        wait_for_fd()
                    else:
                        if total_sent == 0:
                            # We can get here for different reasons, the main
                            # one being 'file' is not a regular mmap(2)-like
                            # file, in which case we'll fall back on using
                            # plain send().
                            raise _GiveupOnSendfile(err)
                        raise err
                else:
                    if sent == 0:
                        break  # EOF
                    offset += sent
                    total_sent += sent
            return total_sent
        finally:
            if total_sent > 0 and hasattr(file, 'seek'):
                file.seek(offset)
else:
    def _sendfile_use_sendfile(sock, file, offset=0, count=None):
        raise _GiveupOnSendfile(
            "sendfile() not available on this platform")


def _sendfile_use_send(sock, file, offset=0, count=None):
    _check_sendfile_params(sock, file, offset, count)
    if sock.gettimeout() == 0:
        raise ValueError("non-blocking sockets are not supported")
    if offset:
        file.seek(offset)
    blocksize = min(count, 8192) if count else 8192
    total_sent = 0
    # localize variable access to minimize overhead
    file_read = file.read
    sock_send = sock.send
    try:
        while True:
            if count:
                blocksize = min(count - total_sent, blocksize)
                if blocksize <= 0:
                    break
            data = memoryview(file_read(blocksize))
            if not data:
                break  # EOF
            while True:
                try:
                    sent = sock_send(data)
                except OSError as err:
                    if err.errno in _RETRY:
                        continue
                    raise
                else:
                    total_sent += sent
                    if sent < len(data):
                        data = data[sent:]
                    else:
                        break
        return total_sent
    finally:
        if total_sent > 0 and hasattr(file, 'seek'):
            file.seek(offset + total_sent)


def _check_sendfile_params(sock, file, offset, count):
    if 'b' not in getattr(file, 'mode', 'b'):
        raise ValueError("file should be opened in binary mode")
    if not sock.type & socket.SOCK_STREAM:
        raise ValueError("only SOCK_STREAM type sockets are supported")
    if count is not None:
        if not isinstance(count, int):
            raise TypeError(
                "count must be a positive integer (got %s)" % repr(count))
        if count <= 0:
            raise ValueError(
                "count must be a positive integer (got %s)" % repr(count))

def sendfile_send(sock, file, offset=0, count=None):
    """sendfile_send(sock, file[, offset[, count]]) -> sent

    API-compatible version of sendfile which does not use the sendfile system
    call. The purpose of this is to allow users to disable usage of sendfile
    (either in the case where https is used on the server and sendfile breaks,
    or some other situation where sendfile is problematic). See the sendfile
    function for more details about arguments.
    """
    return _sendfile_use_send(sock, file, offset, count)


def sendfile(sock, file, offset=0, count=None):
    """sendfile(sock, file[, offset[, count]]) -> sent

    Send a *file* over a connected socket *sock* until EOF is
    reached by using high-performance sendfile(2) and return the
    total number of bytes which were sent.
    *file* must be a regular file object opened in binary mode.
    If sendfile() is not available (e.g. Windows) or file is
    not a regular file socket.send() will be used instead.
    *offset* tells from where to start reading the file.
    If specified, *count* is the total number of bytes to transmit
    as opposed to sending the file until EOF is reached.
    File position is updated on return or also in case of error in
    which case file.tell() can be used to figure out the number of
    bytes which were sent.
    The socket must be of SOCK_STREAM type.
    Non-blocking sockets are not supported.
    """
    try:
        return _sendfile_use_sendfile(sock, file, offset, count)
    except _GiveupOnSendfile:
        return _sendfile_use_send(sock, file, offset, count)