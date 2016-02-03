#******************************************************************************
#
#
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# cwu      22/11/2013  Created
#
""" A python wrapper that interacts with DMF using command line """

import os
import time
import json
import socket
import struct
from subprocess import Popen, PIPE
from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import alert, info

TAPE_ONLY_STATUS = ['NMG', 'OFL', 'PAR'] # these states tell us there are only complete copies of the file currently on tape (no copies on disks)
ERROR_STATUS = ['INV']
MIGRATED_STATUS = ['DUL', 'OFL', 'PAR'] # steady states after at least one migration
RELEASABLE_STATUS = ['DUL'] # 'PAR' is not releasable further as it will make the while file offline
ON_DISK_STATUS = ['DUL', 'REG']


def readDMFStatus(filename):

    """
    Lists file names in long format, giving
    mode, number of links, owner, group, size in bytes, time of last modification, and,
    in parentheses before the file name, the DMF state
    """
    cmd = ['dmls', '-l', filename]
    proc = Popen(cmd, stdout = PIPE, close_fds = True)
    out, err = proc.communicate()
    if proc.returncode != 0:
        raise Exception(out)
    try:
        status = out.split()[7][1:4]
        if status in ERROR_STATUS:
            raise Exception('dmls status error: %s filename: %s' % (status, filename))
        return status
    except IndexError as e:
        raise Exception('dmls output error')


def isFileReleasable(filename):
    """
    To check if the file is releasable from the disk

    return 1 - releasable, 0 - not releasable, Exception - query error
    """
    status = readDMFStatus(filename)
    return 1 if status in RELEASABLE_STATUS else 0

def isFileOffline(filename):
    """
    To check if the file is completely offline, thus no copy is online

    return 1 - on tape, 0 - not on tape, Exception - query error
    """
    return isFileOnTape(filename)

def isFileOnTape(filename):
    """
    To check if the file is completely on tape ONLY, thus no copy is on the disk

    return 1 - on tape, 0 - not on tape, Exception - query error
    """
    status = readDMFStatus(filename)
    return 1 if status in TAPE_ONLY_STATUS else 0

def releaseFiles(filenames):
    """
    Release the disk space of a list of files
    that already have copies on tape

    RETURN  the number files released
    """

    released = []
    cmd = ['dmput', '-r']

    for filename in filenames:
        if isFileReleasable(filename) == 1:
            cmd.append(' ')
            cmd.append(filename)
            released.append(filename)

    proc = Popen(cmd, stdout = PIPE, close_fds = True)
    out, err = proc.communicate()
    if proc.returncode != 0:
        raise Exception(out)

    return released

def pawseyMWAdmget(filelist, host, port, retries = 3, backoff = 5, timeout = 1800):
    """
    issue a dmget which will do a bulk staging of files for a complete observation;
    this function will block until all files are staged or there is a timeout

    filename:    filename to be staged (string)
    host:        host running the pawseydmget daemon (string)
    port:        port of the pawsey / mwadmget daemon (int)
    timeout:     socket timeout in seconds
    retries:     number of times to retry (timeout is halved each retry)
    backoff:     number of seconds to wait before retrying
    """

    # Retry scheme should be a decorator
    while True:
        sock = None
        try:
            files = {'files' : filelist}
            jsonoutput = json.dumps(files)

            val = struct.pack('>I', len(jsonoutput))
            val = val + jsonoutput

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            sock.sendall(val)

            exitcode = struct.unpack('!H', sock.recv(2))[0]
            if exitcode != 0:
                raise Exception('pawseyMWAdmget error with exitcode \
                                %s' % (str(exitcode)))

            # success so exit retry loop
            break

        except Exception as e:
            if retries > 0:
                retries -= 1
                timeout /= 2
                alert('pawseyMWAdmget raised an error: %s, \
                        retrying...' % (str(e)))
                time.sleep(backoff)
            else:
                alert('pawseyMWAdmget raised an error: %s, \
                        no more retries, raising exception!' % (str(e)))
                raise e

        finally:
            if sock:
                sock.close()

def stageFiles(filenames, requestObj = None, serverObj = None):
    """
    Stage a list of files.
    The system will sort files in the order that they are archived on
    the tape volumes for better performance

    RETURN   the number of files staged. Exception on error
    """

    host = 'fe1.pawsey.ivec.org'
    port = 9898

    if serverObj:
        phost = serverObj.getCfg().getFileStagingPlugHost()
        if phost:
            host = phost

        pport = serverObj.getCfg().getFileStagingPlugPort()
        if pport:
            port = pport

    filelist = filenames

    try:
        # if we have a requestObj that implies that its from the
        # RETRIEVE command therefore we should have only one file in the list
        if requestObj and len(filenames) == 1:
            # if the http header contains a list of advised
            # files to prestage then stage these instead
            prestageStr = requestObj.getHttpHdr('prestagefilelist')
            if prestageStr:
                prestageList = json.loads(prestageStr)
                requestedfile = os.path.basename(filelist[0])
                # make sure the file that is being asked for is also
                # in the prestage list of paths
                if not any(requestedfile in s for s in prestageList):
                    alert('ngamsMWAPawseyTapeAPI stageFiles: requested file \
                            %s is not in prestage list' % requestedfile)
                    filelist = list(filenames)
                    filelist.append(prestageList)
                else:
                    info(3, 'ngamsMWAPawseyTapeAPI stageFiles: requested file \
                        %s is in prestage list, prestaging files now' % requestedfile)
                    filelist = prestageList
        else:
            info(3, 'ngamsMWAPawseyTapeAPI stageFiles: prestagefilelist \
                    not found in http header, ignoring')

    except Exception as exp:
        alert('ngamsMWAPawseyTapeAPI stageFiles: prestagefilelist error %s' % (str(exp)))

    pawseyMWAdmget(filelist, host, port)
    return len(filelist)
