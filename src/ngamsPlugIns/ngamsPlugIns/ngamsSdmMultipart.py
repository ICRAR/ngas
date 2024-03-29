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
# "@(#) $Id: ngamsSdmMultipart.py,v 1.9 2010/06/29 16:03:42 mbauhofe Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  2008/04/10  Created
# mbauhofe  2010/06/29  Corrected exception handling.
#
"""
This Data Archiving Plug-In is used to handle reception and processing of SDM multipart related message files
containing Content-Location UIDs.

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

# Update for the new assignment of archive IDs (backwards compatible)
UID_EXPRESSION = re.compile(r"^[uU][iI][dD]://[aAbBcCzZxX][0-9,a-z,A-Z]+(/[xX][0-9,a-z,A-Z]+){2}(#\w{1,}|/\w{0,}){0,}$")

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
            # We only want to read the primary header block lines into the parser
            # According to section 3.1 of RFC822:
            # A message consists of header fields and, optionally, a body. The body is simply a sequence of lines
            # containing ASCII characters. It is separated from the headers by a null line (i.e., a  line with nothing
            # preceding the CRLF).
            feedparser = email.parser.FeedParser()
            for line in fo:
                line = line.decode(encoding="utf-8")
                if line.startswith("\r") or line.startswith("\n") or line.startswith("--"):
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
        raise Exception(genLog("NGAMS_ER_DAPI_BAD_FILE", [filename, PLUGIN_ID, "Failed to parse MIME message"]))

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
        alma_uid = alma_uid.rstrip("/")
        file_id = alma_uid
        final_filename = alma_uid.replace("/", ":")
    except Exception as e:
        raise Exception(genLog("NGAMS_ER_DAPI_BAD_FILE",
                               [filename, PLUGIN_ID, "Problem constructing final file name: " + str(e)]))

    return file_id, final_filename, file_type


def ngamsSdmMultipart(ngams_server, request_properties):
    """
    Data Archiving Plug-In to handle archiving of SDM multipart related message files containing ALMA UIDs in the
    Content-Location mime parameter
    :param ngams_server: Reference to NG/AMS Server Object (ngamsServer)
    :param request_properties: NG/AMS request properties object (ngamsReqProps)
    :return: Standard NG/AMS Data Archiving Plug-In Status as generated by: ngamsPlugInApi.genDapiSuccessStat()
             (ngamsDapiStatus)
    """
    # For now the exception handling is pretty basic:
    # If something goes wrong during the handling it is tried to move the temporary file to the bad-files directory
    logger.info("SDM multipart plug-in handling data for file: %s", os.path.basename(request_properties.getFileUri()))

    disk_info = request_properties.getTargDiskInfo()
    staging_filename = request_properties.getStagingFilename()

    file_id, final_filename, file_format = specific_treatment(staging_filename)

    if request_properties.hasHttpPar("file_id"):
        file_id = request_properties.getHttpPar("file_id")

    #if request_properties.hasHttpPar("file_version"):
    #    file_version = request_properties.getHttpPar("file_version")

    logger.debug("SDM multipart plug-in processing request for file with URI %s, file_format=%s, file_id=%s, "
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
