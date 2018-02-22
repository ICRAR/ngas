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
# "@(#) $Id: ngamsLib.py,v 1.13 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/05/2001  Created
#
"""
Base module that contains various utility functions used for the
NG/AMS implementation.

The functions in this module can be used in all the NG/AMS code.
"""

import gzip
import logging
import os
import shutil
import socket
import string
import urllib

from six.moves import cPickle # @UnresolvedImport

from .ngamsCore import genLog, NGAMS_UNKNOWN_MT, rmFile


logger = logging.getLogger(__name__)

def hidePassword(fileUri):
    """
    Hide password in a URI by replacing it by asterix characters.

    fileUri:   File URI (string).

    Returns:   URI with password blanked out (string).
    """
    if not fileUri:
        return fileUri

    tmpUri = urllib.unquote(fileUri)
    if "ftp://" not in tmpUri or '@' not in tmpUri:
        return fileUri

    # ARCHIVE?filename="ftp://jknudstr:*****@host//home/...
    lst1 = string.split(tmpUri,"@")
    lst2 = string.split(lst1[0], ":")
    return lst2[0] + ":" + lst2[1] + ":*****@" + lst1[1]


def parseHttpHdr(httpHdr):
    """
    Parse an HTTP header like this:

      <par>=<val>; <par>=<val>

    httpHdr:     HTTP header (string).

    Returns:     List with the contents:

                 [[<par>, <value>], [<par>, <value>], ...]
    """
    retDic = {}
    els = string.split(httpHdr, ";")
    for el in els:
        subEls = string.split(el, "=")
        key = subEls[0].strip("\" ")
        if (len(subEls) > 1):
            value = subEls[1].strip("\" ")
        else:
            value = ""
        retDic[key] = value
    return retDic


def httpMsgObj2Dic(httpMessageObj):
    """
    Stores the HTTP header information of mimetools.Message object
    in a dictionary, whereby the header names are keys.

    httpMessageObj:     Message object (mimetools.Message).

    Returns:            Dictionary with HTTP header information (dictionary).
    """
    httpHdrDic = {}
    for httpHdr in str(httpMessageObj).split("\r\n"):
        if (httpHdr != ""):
            idx = httpHdr.index(":")
            hdr, val = [httpHdr[0:idx].lower(), httpHdr[(idx + 2):]]
            httpHdrDic[hdr] = val
    return httpHdrDic


def getCompleteHostName():
    """
    Return the complete host name, i.e., including the name of the domain.

    Returns:   Host name for this NGAS System (string).
    """
    return socket.getfqdn()


def getDomain():
    """
    Return the name of the domain.

    Returns: Domain name or "" if unknown (string).
    """
    fqdn = socket.getfqdn()
    if '.' not in fqdn:
        return ""
    return '.'.join(fqdn.split('.')[1:])


def makeFileReadOnly(completeFilename):
    """
    Make a file read-only.

    completeFilename:    Complete name of file (string).

    Returns:             Void.
    """
    os.chmod(completeFilename, 0o444)
    logger.debug("File: %s made read-only", completeFilename)


def fileWritable(filename):
    """
    Return 1 if file is writable, otherwise return 0.

    filename:   Name of file (path) to check (string).

    Returns:    1 if file is writable, otherwise 0 (integer).
    """
    return os.access(filename, os.W_OK)


def parseRawPlugInPars(rawPars):
    """
    Parse the plug-in parameters given in the NG/AMS Configuration
    and return a dictionary with the values.

    rawPars:    Parameters given in connection with the plug-in in
                the configuration file (string).

    Returns:    Dictionary containing the parameters from the plug-in
                parameters as keys referring to the corresponding
                value of these (dictionary).
    """
    # Plug-In Parameters. Expect:
    # "<field name>=<field value>[,field name>=<field value>]"
    parDic = {}
    pars = string.split(rawPars, ",")
    for par in pars:
        if (par != ""):
            try:
                parVal = string.split(par, "=")
                par = parVal[0].strip()
                parDic[par] = parVal[1].strip()
                logger.debug("Found plug-in parameter: %s with value: %s",
                             par, parDic[par])
            except:
                errMsg = genLog("NGAMS_ER_PLUGIN_PAR", [rawPars])
                raise Exception(errMsg)
    logger.debug("Generated parameter dictionary: %s", str(parDic))
    return parDic


def detMimeType(mimeTypeMaps,
                filename,
                noException = 0):
    """
    Determine mime-type of a file, based on the information in the
    NG/AMS Configuration and the filename (extension) of the file.

    mimeTypeMaps:  See ngamsConfig.getMimeTypeMappings() (list).

    filename:      Filename (string).

    noException:   If the function should not throw an exception
                   this parameter should be 1. In that case it
                   will return NGAMS_UNKNOWN_MT (integer).

    Returns:       Mime-type (string).
    """
    # Check if the extension as ".<ext>" is found in the filename as
    # the last part of the filename.
    logger.debug("Determining mime-type for file with URI: %s ...", filename)
    found = 0
    mimeType = ""
    for map in mimeTypeMaps:
        ext = "." + map[1]
        idx = string.find(filename, ext)
        if ((idx != -1) and ((idx + len(ext)) == len(filename))):
            found = 1
            mimeType = map[0]
            break
    if ((not found) and noException):
        return NGAMS_UNKNOWN_MT
    elif (not found):
        errMsg = genLog("NGAMS_ER_UNKNOWN_MIME_TYPE1", [filename])
        raise Exception(errMsg)
    else:
        logger.debug("Mime-type of file with URI: %s determined: %s",
                     filename, mimeType)

    return mimeType


def getSubscriberId(subscrUrl):
    """
    Generate the Subscriber ID from the Subscriber URL.

    subscrUrl:   Subscriber URL (string).

    Returns:     Subscriber ID (string).
    """
    return subscrUrl.split("?")[0]


def fileRemovable(filename):
    """
    The function checks if a file is removable or not. In case yes, 1 is
    returned otherwise 0 is returned. If the file is not available 0 is
    returned. If the file is not existing 2 is returned.

    filename:     Complete name of file (string).

    Returns:      Indication if file can be removed or not (integer/0|1|2).
    """
    # We simply carry out a temporary move of the file.
    tmpFilename = filename + "_tmp"
    try:
        if (not os.path.exists(filename)): return 2
        shutil.move(filename, tmpFilename)
        shutil.move(tmpFilename, filename)
        return 1
    except:
        return 0


def createObjPickleFile(filename,
                        object):
    """
    Create a file containing the pickled image of the object given.

    filename:    Name of pickle file (string).

    object:      Object to be pickled (<object>)

    Returns:     Void.
    """
    logger.debug("createObjPickleFile() - creating pickle file %s ...", filename)
    rmFile(filename)
    with open(filename, "w") as pickleFo:
        cPickle.dump(object, pickleFo)


def loadObjPickleFile(filename):
    """
    Load/create a pickled object from a pickle file.

    filename:    Name of pickle file (string).

    Returns:     Reconstructed object (<object>).
    """
    with open(filename, "r") as pickleFo:
        return cPickle.load(pickleFo)

def genFileKey(diskId,
               fileId,
               fileVersion):
    """
    Generate a unique key identifying a file.

    diskId:         Disk ID (string).

    fileId:         File ID (string).

    fileVersion:    File Version (integer).

    Returns:        File key (string).
    """
    if (diskId):
        return str("%s_%s_%s" % (diskId, fileId, str(fileVersion)))
    else:
        return str("%s_%s" % (fileId, str(fileVersion)))


def trueArchiveProxySrv(cfgObj):
    """
    Return 1 if an NG/AMS Server is configured as a 'TrueT Archive Proxy
    Server'. I.e., no local archiving will take place, all Archive Requests
    received, will be forwarded to the sub-nodes specified.

    The exact criterias for classifying a node as a True Archive Proxy Server
    are as follows:

      - No Storage Sets are defined.
      - All Streams definition have a set of NAUs defined.

    cfgObj:   Instance of NG/AMS Configuration Object (ngamsConfig).

    Returns:   1 if the server is a True Archive Proxy Server (integer/0|1).
    """
    trueArchProxySrv = 1
    if (cfgObj.getStorageSetList() != []):
        trueArchProxySrv = 0
    else:
        for streamObj in cfgObj.getStreamList():
            if (streamObj.getStorageSetIdList() != []):
                trueArchProxySrv = 0
                break
            elif (streamObj.getHostIdList() == []):
                trueArchProxySrv = 0
                break
    return trueArchProxySrv


def logicalStrListSort(strList):
    """
    Sort a list of strings, such that strings in the list that might be
    lexigraphically smaller than other, but logically larger, are found
    in the end of the list. E.g.:

       ['3','11','1','10','2']

    - becomes:

      ['1','2','3','10','11']

    - end not:

      ['1','10,'11','2','3']

    Returns:   Sorted list (list).
    """
    maxLen = max(map(len, strList))
    estretched_strings = [(maxLen - len(s)) * " " + s for s in strList]
    estretched_strings.sort()
    return estretched_strings


class GzipFile(gzip.GzipFile):
    """
    Our version of GzipFile which calculates (if indicated) a given checksum on
    the *compressed* data as it gets written into the output file.
    """

    def __init__(self, filename, mode, crc_info=None):

        fileobj = open(filename, mode)

        # Checksum requested, create the wrapper for checksum calculation at
        # write time and pass the wrapped object down instead
        if crc_info:

            class wrapper(object):

                def __init__(self, f):
                    self.f = f
                    self.crc = crc_info.init
                    self.mode = mode

                def write(self, data):
                    self.crc = crc_info.method(data, self.crc)
                    return self.f.write(data)

                def flush(self):
                    return self.f.flush()

                def close(self):
                    self.crc = crc_info.final(self.crc)
                    return self.f.close()

            # set self.myfileobj so gzip.GzipFile.close() closes fileobj
            fileobj = self.myfileobj = wrapper(fileobj)

        # The "gzip" command defaults to compression level 6
        gzip.GzipFile.__init__(self, fileobj=fileobj, compresslevel=6)

def gzip_compress(fin, fout_name, block_size, crc_info=None):
    """
    Compresses the contents read from `fin` into file `fout_name`. Reading from
    `fin` is done by reading `block_size` bytes at a time.

    If `crc_init` and `crc_m` are provided then a checksum on the compressed
    data is calculated as data gets compressed using `crc_m` as the checksum
    method, and `crc_init` as the initial checksum value.
    """
    with GzipFile(fout_name, 'wb', crc_info=crc_info) as fout:
        if crc_info:
            fileobj = fout.fileobj
        shutil.copyfileobj(fin, fout, block_size)

    if crc_info:
        return fileobj.crc

# EOF
