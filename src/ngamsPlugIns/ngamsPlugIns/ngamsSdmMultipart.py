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
# "@(#) $Id: ngamsSdmMultipart.py,v 1.9 2010/06/29 16:03:42 mbauhofe Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  2008/04/10  Created
# mbauhofe  2010/06/29  Corrected exception handling.
#
"""
This Data Archiving Plug-In is used to handle reception and processing
of SDM multipart related message files containing Content-Location UIDs.

Note, that the plug-in is implemented for the usage for ALMA. If used in other
contexts, a dedicated plug-in matching the individual context should be
implemented and NG/AMS configured to use it.
"""

import cgi
from datetime import date
import logging
import os
import re
import rfc822

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import genLog

# _EXT = '.msg'
_PLUGIN_ID = __name__
logger = logging.getLogger(__name__)


def specific_treatment(file_object):
    """
    Method contains the specific treatment of the file passed from NG/AMS.

    fo:         File object

    Returns:    (file_id, finalFileName, type);
                The finalFileName is a string containing the name of the final
                file without extension. type is the mime-type from the header.
    """

    filename = file_object.name
    try:
        message = rfc822.Message(file_object)
        file_type, file_type_parameters = cgi.parse_header(message["Content-Type"])
    except Exception as ex:
        error = "Parsing of mime message failed: " + str(ex)
        error_message = genLog("NGAMS_ER_DAPI_BAD_FILE",
                               [os.path.basename(filename), _PLUGIN_ID, error])
        raise Exception(error_message)

    try:
        # CAUTION!!! Parsing the_header returns stuff in lower case
        # That is why it is not used here
        alma_uid = message["alma-uid"]
    except Exception:
        try:
            alma_uid = message["Content-Location"]
        except Exception:
            error = "Mandatory alma-uid or Content-Location parameter not found in mime header!"
            error_message = genLog("NGAMS_ER_DAPI_BAD_FILE",
                            [os.path.basename(filename), _PLUGIN_ID, error])
            raise Exception(error_message)

    # This matches the old UID of type X0123456789abcdef/X01234567
    # uid_expression = re.compile("[xX][0-9,a-f]{3,}/[xX][0-9,a-f]{1,}$")

    # This matches the new UID structure uid://X1/X2/X3#kdgflf
    # uid_expression = re.compile("^[uU][iI][dD]:/(/[xX][0-9,a-f,A-F]+){3}(#\w{1,}|/\w{0,}){0,}$")

    # Update for the new assignment of archiveIds (backwards compatible)
    uid_expression = re.compile("^[uU][iI][dD]://[aAbBcCzZxX][0-9,a-z,A-Z]+(/[xX][0-9,a-z,A-Z]+){2}(#\w{1,}|/\w{0,}){0,}$")

    if not uid_expression.match(alma_uid):
        error = "Invalid alma-uid found in Content-Location: " + alma_uid
        error_message = genLog("NGAMS_ER_DAPI_BAD_FILE",
                               [os.path.basename(filename), _PLUGIN_ID, error])
        raise Exception(error_message)

    # Now, build final filename. We do that by looking for the UID in
    # the message mime-header.
    #
    # The final filename is built as follows: <almaUidR>.<ext>
    # where almaUidR has the slash character in the UID replaced by colons.
    try:
        # Get rid of the 'uid://' and of anything following a '#' sign
        alma_uid = alma_uid.split('//', 2)[1].split('#')[0]

        # Remove trailing '/'
        if alma_uid[-1] == '/':
            alma_uid = alma_uid[:-1]

        file_id = alma_uid
        final_filename = alma_uid.replace('/', ':')

        # Have no idea why, but the extension is added somewhere else
        # if os.path.splitext(final_filename)[-1] != _EXT:
        #     final_filename += _EXT
    except Exception as ex:
        error = "Problem constructing final file name: " + str(ex)
        error_message = genLog("NGAMS_ER_DAPI_BAD_FILE",
                        [os.path.basename(filename), _PLUGIN_ID, error])
        raise Exception(error_message)

    return file_id, final_filename, file_type


def ngamsSdmMultipart(server_object, request_object):
    """
    Data Archiving Plug-In to handle archiving of SDM multipart related
    message files containing ALMA UIDs in the Content-Location mime parameter.

    server_object:  Reference to NG/AMS Server Object (ngamsServer).

    request_object: NG/AMS request properties object (ngamsReqProps)

    Returns:        Standard NG/AMS Data Archiving Plug-In Status
                    as generated by: ngamsPlugInApi.genDapiSuccessStat()
                    (ngamsDapiStatus)
    """

    # For now the exception handling is pretty basic:
    # If something goes wrong during the handling it is tried to
    # move the temporary file to the Bad Files Area of the disk.
    logger.info("Plug-In handling data for file: %s",
                os.path.basename(request_object.getFileUri()))
    disk_info = request_object.getTargDiskInfo()
    staging_filename = request_object.getStagingFilename()
    # extension = os.path.splitext(staging_filename)[1][1:]

    file_object = open(staging_filename, "r")
    file_id, final_filename, file_format = specific_treatment(file_object)

    if request_object.hasHttpPar('file_id'):
        file_id = request_object.getHttpPar('file_id')

    file_object.close()
    try:
        # Compress the file
        uncompressed_size = ngamsPlugInApi.getFileSize(staging_filename)
        compression = ""

        # logger.info("Compressing file %s using %s compression",
        #             staging_filename, compression)
        # exit_code, std_out = ngamsPlugInApi.execCmd("{0} {1}".format(
        #     compression, staging_filename))
        # if exit_code != 0:
        #     error_message = _PLUGIN_ID + ": Problems during archiving! " + \
        #                     "Compressing the file failed"
        #     raise Exception(error_message)
        # staging_filename = staging_filename + ".Z"
        # logger.info("Finished compressing staging file %s", staging_filename)

        # Remember to update the temporary file name in the request
        # properties object
        request_object.setStagingFilename(staging_filename)

        # # ToDo: Handling of non-existing fileId
        # if file_id == -1:
        #     file_id = ngamsPlugInApi.genNgasId(server_object.getCfg())

        today = date.today().isoformat()
        file_version, relative_path, relative_filename, complete_filename,\
        file_exists = ngamsPlugInApi.genFileInfo(server_object.getDb(),
                                                 server_object.getCfg(),
                                                 request_object, disk_info,
                                                 staging_filename, file_id,
                                                 final_filename, [today])

        logger.debug("Generating status ...")
        if not file_format:
            file_format = ngamsPlugInApi.determineMimeType(server_object.getCfg(),
                                                           staging_filename)

        file_size = ngamsPlugInApi.getFileSize(staging_filename)
        return ngamsPlugInApi.genDapiSuccessStat(disk_info.getDiskId(),
                                                 relative_filename, file_id,
                                                 file_version, file_format,
                                                 file_size, uncompressed_size,
                                                 compression, relative_path,
                                                 disk_info.getSlotId(),
                                                 file_exists,
                                                 complete_filename)
    except Exception as ex:
        error_message = genLog("NGAMS_ER_DAPI_BAD_FILE",
                               [os.path.basename(staging_filename),
                                _PLUGIN_ID, str(ex)])
        raise Exception(error_message)
