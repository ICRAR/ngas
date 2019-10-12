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
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  26/02/2007  Created
#
"""
Volume-related utilities, including the ngas-prepare-volume command line tool
"""

import argparse
import base64
import hashlib
import logging
import os
import sys
import time

import six

from ngamsLib import utils
from ngamsLib.ngamsCore import getHostName


logger = logging.getLogger(__name__)

if sys.version_info[0] == 3:
    raw_input = input

NGAS_VOL_INFO_FILE = ".ngas_volume_info"
NGAS_VOL_INFO_IGNORE = "IGNORE"
NGAS_VOL_INFO_UNDEF = "UNDEFINED"
NGAS_VOL_INFO_UNDEF_NO = -1

# Parameters.
NGAS_VOL_INFO_ID = "DiskId"
NGAS_VOL_INFO_TYPE = "Type"
NGAS_VOL_INFO_MANUFACT = "Manufacturer"


def writeVolInfoFile(fname, dic):
    """Write dictionary of parameters into a file, b64-encoded"""
    newDic = {}
    if os.path.exists(fname):
        newDic = loadVolInfoFile(fname)
    newDic.update(dic)
    volInfoBuf = ""
    for key, val in sorted(newDic.items()):
        volInfoBuf += "%s = %s\n" % (key, val)
    with open(fname, "wb+") as f:
        f.write(base64.b64encode(six.b(volInfoBuf)))


def loadVolInfoFile(fname):
    """Load an NGAS Volume Info file and return it as a dictionary"""
    with open(fname, "rb") as fo:
        volInfoBuf = utils.b2s(base64.b64decode(fo.read()))
    dic = {}
    for line in volInfoBuf.split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        par, val = line.split("=")
        dic[par.strip()] = val.strip()
    return dic

def generate_disk_id(voldir):
    pat = "%s_%s_%.6f" % (getHostName(), voldir, time.time())
    return hashlib.md5(six.b(pat)).hexdigest()

def prepare_volume_info_file(voldir, disk_id=None, disk_type=None,
                             manufacturer=None, overwrite=False,
                             check_func=None):

    # Initial checks
    info_fname = os.path.join(voldir, NGAS_VOL_INFO_FILE)
    if not os.path.isdir(voldir):
        raise ValueError(
            'Path does not exist, or is not a directory: %s' % voldir)
    exists = os.path.exists(info_fname)
    if exists and not overwrite:
        msg = ("Specified volume path: %s already contains an NGAS Volume Info File")
        raise ValueError(msg % voldir)

    if exists:
        info = loadVolInfoFile(info_fname)
        info[NGAS_VOL_INFO_TYPE] = disk_type or info[NGAS_VOL_INFO_TYPE]
        info[NGAS_VOL_INFO_MANUFACT] = manufacturer or info[NGAS_VOL_INFO_MANUFACT]
    else:
        info = {}
        info[NGAS_VOL_INFO_ID] = disk_id or generate_disk_id(voldir)
        info[NGAS_VOL_INFO_TYPE] = disk_type or 'UNDEFINED'
        info[NGAS_VOL_INFO_MANUFACT] = manufacturer or 'UNDEFINED'

    if check_func:
        check_func(info)

    # Write/update the NGAS Volume Info File.
    logger.info("Writing/updating NGAS Volume Info File: %s ..." % info_fname)
    writeVolInfoFile(info_fname, info)
    logger.info("Wrote/updated NGAS Volume Info File: %s" % info_fname)

    # Ensure volume/info_file is owned by the current user, r/o for the rest
    uid, gid = os.getuid(), os.getgid()
    os.chown(voldir, uid, gid)
    os.chmod(voldir, 0o755)
    os.chown(info_fname, uid, gid)
    os.chmod(info_fname, 0o644)


def prepare_volume():
    """Entry point for the ngas-prepare-volume tool"""

    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('volume_path',
                        help='The path of the volume as mounted on the system')
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help='Overwrite an existing NGAS Volume Info File')
    parser.add_argument('-i', '--id',
                        help='Disk ID. Defaults to auto-generated, unique ID')
    parser.add_argument('-t', '--type',
                        help='Type of the volume')
    parser.add_argument('-m', '--manufacturer',
                        help='Name of the disk manufacturer')
    parser.add_argument('-y', '--assume-yes', action='store_true',
                        help='Automatically answer "yes" to questions')

    opts = parser.parse_args()
    voldir = opts.volume_path

    def _dump(info):
        buf = "\n\nDisk parameters for disk:\n\n"
        buf += "Path:         %s\n" % voldir
        for what, key in (('Disk ID', NGAS_VOL_INFO_ID),
                          ('Type', NGAS_VOL_INFO_TYPE),
                          ('Manufacturer', NGAS_VOL_INFO_MANUFACT)):
            buf += "%-14s%s\n" % (what + ":", info[key])
        print(buf)

    def ask(info, key, what):
        value = info[key]
        msg = "Enter %s [%s]: " % (what, value)
        newType = raw_input(msg)
        if newType:
            value = newType
        info[key] = value

    def check(info):
        # Generate/check parameters, get user confirmation.
        if not opts.assume_yes:
            while True:
                _dump(info)
                choice = raw_input("Are these parameters correct (y/N)? ")
                if choice and choice.lower() not in ('n', 'no'):
                    break
                print("\n")
                ask(info, NGAS_VOL_INFO_TYPE, 'disk type')
                ask(info, NGAS_VOL_INFO_MANUFACT, 'manufacturer')

    prepare_volume_info_file(voldir, disk_id=opts.disk_id,
                             disk_type=opts.disk_type,
                             manufacturer=opts.manufacturer,
                             overwrite=opts.overwrite, check_func=check)


# Still allow to execute as a module
if __name__ == '__main__':
    prepare_volume()
