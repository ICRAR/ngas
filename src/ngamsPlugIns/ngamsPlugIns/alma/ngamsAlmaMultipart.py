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
# *****************************************************************************
#
# "@(#) $Id: ngamsAlmaMultipart.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  2004/10/01  Created
#
"""
This Data Archiving Plug-In is used to handle reception and processing of ALMA multipart related message files
containing ALMA UIDs.

Note that the plug-in is implemented for the usage for ALMA. If used in other contexts, a dedicated plug-in matching
the individual context should be implemented and NG/AMS configured to use it.
"""

import email.parser
import logging
import os
import re

from ngamsLib import ngamsPlugInApi
from ngamsLib import ngamsCore
from ngamsLib.ngamsCore import genLog

PLUGIN_ID = __name__

# This matches the new UID structure uid://X1/X2/X3#kdgflf
UID_EXPRESSION = re.compile(r"^[uU][iI][dD]:/(/[xX][0-9,a-f,A-F]+){3}(#\w{1,}){0,}$")

logger = logging.getLogger(__name__)


def parse_multipart_primary_header(file_path):
    """
    Parses email MIME document file and extracts the primary header elements
    :param file_path: File path
    :return: email message object
    """
    filename = os.path.basename(file_path)
    try:
        # We read file using binary and decode to utf-8 later to ensure python 2/3 compatibility
        with open(file_path, 'rb') as fo:
            # Verify the file uses MIME format
            line = fo.readline().decode(encoding="utf-8")
            first_line = line.lower()
            if not first_line.startswith("message-id") and not first_line.startswith("mime-version"):
                raise Exception(genLog("NGAMS_ER_DAPI_BAD_FILE", [filename, PLUGIN_ID, "File is not MIME file format"]))
            # Read primary header block lines into the parser
            feedparser = email.parser.FeedParser()
            feedparser.feed(line)
            for line in fo:
                line = line.decode(encoding="utf-8")
                if line.startswith("\n"):
                    continue
                if line.startswith("--"):
                    break
                feedparser.feed(line)
            return feedparser.close()
    except Exception as e:
        raise Exception(genLog("NGAMS_ER_DAPI_BAD_FILE", [filename, PLUGIN_ID, "Failed to open file: " + str(e)]))


def specific_treatment(file_path):
    """
    Method contains the specific treatment of the file passed from NG/AMS
    :param file_path: File path
    :return: (file_id, final_filename, file_type); The final_filename is a string containing the name of the final
             file without extension. file_type is the mime-type from the header.
    """
    filename = os.path.basename(file_path)
    mime_message = parse_multipart_primary_header(file_path)
    if mime_message is None:
        raise Exception(genLog("NGAMS_ER_DAPI_BAD_FILE",
                               [filename, PLUGIN_ID, "Failed to parse MIME message"]))

    file_type = mime_message.get_content_type()
    alma_uid = mime_message["alma-uid"]
    if alma_uid is None:
        alma_uid = mime_message["Content-Location"]
    if alma_uid is None:
        raise Exception(genLog("NGAMS_ER_DAPI_BAD_FILE",
                               [filename, PLUGIN_ID,
                                "Mandatory 'alma-uid' and/or 'Content-Location' parameter not found in MIME header!"]))

    if UID_EXPRESSION.match(alma_uid) is None:
        raise Exception(genLog("NGAMS_ER_DAPI_BAD_FILE",
                               [filename, PLUGIN_ID, "Invalid alma-uid found in Content-Location: " + alma_uid]))

    # Now, build final filename. We do that by looking for the UID in the message mime-header.
    # The final filename is built as follows: <ALMA-UID>.<EXT> where ALMA-UID has the slash character in the UID
    # replaced by colons.
    try:
        # Get rid of the 'uid://' and of anything following a '#' sign
        alma_uid = alma_uid.split("//", 2)[1].split("#")[0]
        alma_uid = alma_uid.replace("x", "X")
        file_id = alma_uid
        final_filename = alma_uid.replace("/", ":")
    except Exception as e:
        raise Exception(genLog("NGAMS_ER_DAPI_BAD_FILE",
                               [filename, PLUGIN_ID, "Problem constructing final file name: " + str(e)]))

    return file_id, final_filename, file_type


def ngamsAlmaMultipart(ngams_server, request_properties):
    """
    Data Archiving Plug-In to handle archiving of ALMA multipart related message files containing ALMA UIDs
    :param ngams_server: Reference to NG/AMS Server Object (ngamsServer)
    :param request_properties: NG/AMS request properties object (ngamsReqProps)
    :return: Standard NG/AMS Data Archiving Plug-In Status as generated by: ngamsPlugInApi.genDapiSuccessStat()
             (ngamsDapiStatus)
    """
    # For now the exception handling is pretty basic:
    # If something goes wrong during the handling it is tried to move the temporary file to the bad-files directory
    logger.info("ALMA multipart plug-in handling data for file: %s", os.path.basename(request_properties.getFileUri()))

    disk_info = request_properties.getTargDiskInfo()
    staging_filename = request_properties.getStagingFilename()

    file_id, final_filename, file_format = specific_treatment(staging_filename)

    #if request_properties.hasHttpPar("file_id"):
    #    file_id = request_properties.getHttpPar("file_id")

    #if request_properties.hasHttpPar("file_version"):
    #    file_version = request_properties.getHttpPar("file_version")

    logger.debug("ALMA multipart plug-in processing request for file with URI %s, file_format=%s, file_id=%s, "
                 "final_filename=%s", request_properties.getFileUri(), file_format, file_id, final_filename)

    try:
        # Compression parameters
        uncompressed_size = ngamsPlugInApi.getFileSize(staging_filename)
        compression = ""

        # Remember to update the temporary file name in the request properties object
        request_properties.setStagingFilename(staging_filename)

        today = ngamsCore.toiso8601(fmt=ngamsCore.FMT_DATE_ONLY)
        file_version, relative_path, relative_filename, complete_filename, file_exists = \
            ngamsPlugInApi.genFileInfo(ngams_server.getDb(), ngams_server.getCfg(), request_properties, disk_info,
                                       staging_filename, file_id, final_filename, [today])

        # Make sure the format is defined
        if not file_format:
            file_format = ngamsPlugInApi.determineMimeType(ngams_server.getCfg(), staging_filename)

        file_size = ngamsPlugInApi.getFileSize(staging_filename)

        return ngamsPlugInApi.genDapiSuccessStat(disk_info.getDiskId(), relative_filename, file_id, file_version,
                                                 file_format, file_size, uncompressed_size, compression, relative_path,
                                                 disk_info.getSlotId(), file_exists, complete_filename)
    except Exception as e:
        raise Exception(genLog("NGAMS_ER_DAPI_BAD_FILE",
                               [staging_filename, PLUGIN_ID, "Problem processing file in staging area: " + str(e)]))
