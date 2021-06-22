#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
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
# "@(#) $Id: ngamsDAPIMirroring.py,v 1.5 2012/01/30 19:06:15 amanning Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# awicenec  2008/04/10  Created
#
"""
This Data Archiving Plug-In is used to handle reception and processing of SDM multipart related message files
containing Content-Location UIDs.

Note that the plug-in is implemented for the usage for ALMA. If used in other contexts, a dedicated plug-in matching
the individual context should be implemented and NG/AMS configured to use it.
"""

import cgi
import logging
import os
import re
import rfc822

from ngamsLib import ngamsPlugInApi
from ngamsLib.ngamsCore import genLog, toiso8601, FMT_DATE_ONLY

logger = logging.getLogger(__name__)

_EXT = '.msg'
_PLUGIN_ID = __name__


def specific_treatment(file_object):
    """
    Method contains the specific treatment of the file passed from NG/AMS
    :param file_object: File object
    :return: (file_id, finalFileName, type); The finalFileName is a string containing the name of the final file without extension. type is the mime-type from the header.
    """
    filename = file_object.name
    uid_template = re.compile("^[uU][iI][dD]://[aAbBcCzZxX][0-9,a-z,A-Z]+(/[xX][0-9,a-z,A-Z]+){2}(#\w{1,}|/\w{0,}){0,}$")

    try:
        message = rfc822.Message(file_object)
        type, type_params = cgi.parse_header(message["Content-Type"])
    except Exception as e:
        error_message = "Parsing of mime message failed: " + str(e)
        exception_message = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename), _PLUGIN_ID, error_message])
        raise Exception(exception_message)

    try:
        alma_uid = message["alma-uid"]
    except:
        try:
            alma_uid = message["Content-Location"]
        except:
            error_message = "Mandatory alma-uid or Content-Location parameter not found in mime header!"
            exception_message = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename), _PLUGIN_ID, error_message])
            raise Exception(exception_message)

    if not uid_template.match(alma_uid):
        error_message = "Invalid alma-uid found in Content-Location: " + alma_uid
        exception_message = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename), _PLUGIN_ID, error_message])
        raise Exception(exception_message)

    try:
        alma_uid = alma_uid.split('//',2)[1].split('#')[0]
        if alma_uid[-1] == '/':
            alma_uid = alma_uid[:-1]
        file_id = alma_uid
        final_file_name = alma_uid.replace('/',':')
    except Exception as e:
        error_message = "Problem constructing final file name: " + str(e)
        exception_message = genLog("NGAMS_ER_DAPI_BAD_FILE", [os.path.basename(filename),_PLUGIN_ID, error_message])
        raise Exception(exception_message)

    return file_id, final_file_name, type


def generate_file_info(ngams_config, target_disk_info, staging_filename, file_version, base_filename,
                       subdirectory_list=[], additional_extension_list=[]):

    relative_path = ngams_config.getPathPrefix()

    for subdirectory in subdirectory_list:
        if relative_path != "":
            relative_path += "/" + subdirectory

    relative_path = relative_path + "/" + str(file_version)
    relative_path = relative_path.strip("/")
    complete_path = os.path.normpath(target_disk_info.getMountPoint() + "/" + relative_path)
    extension = os.path.basename(staging_filename).split(".")[-1]
    new_filename = base_filename

    if not base_filename.endswith(extension):
        new_filename = new_filename + "." + extension

    for additional_extension in additional_extension_list:
        if additional_extension.strip() != "":
            new_filename += "." + additional_extension

    complete_filename = os.path.normpath(complete_path + "/" + new_filename)
    relative_filename = os.path.normpath(relative_path + "/" + new_filename)
    logger.debug("Target name for file is: %s", complete_filename)

    # We already know that the file does not exist
    file_exists = 0
    return relative_path, relative_filename, complete_filename, file_exists


def ngamsGeneric(ngams_server, request_properties):
    """
    Data Archiving Plug-In to handle archiving of SDM multipart related message files containing ALMA UIDs in the
    Content-Location mime parameter or any other kind of file
    :param ngams_server: Reference to NG/AMS Server Object (ngamsServer)
    :param request_properties: NG/AMS request properties object (ngamsReqProps)
    :return: Standard NG/AMS Data Archiving Plug-In Status as generated by: ngamsPlugInApi.genDapiSuccessStat()
    (ngamsDapiStatus)
    """
    logger.debug("Mirroring plug-in handling data for file: %s", os.path.basename(request_properties.getFileUri()))

    # Create the file
    disk_info = request_properties.getTargDiskInfo()
    staging_filename = request_properties.getStagingFilename()
    # extension = os.path.splitext(staging_filename)[1][1:]

    # request_properties format: /MIRRARCHIVE?mime_type=application/x-tar&filename=...
    if request_properties.getMimeType():
        file_format = request_properties.getMimeType()
    else:
        raise Exception("mime_type paremeter not specified in MIRRARCHIVE request")

    # File URI format: http://ngasbe03.aiv.alma.cl:7777/RETRIEVE?disk_id=59622720f79296473f6106c15e5c2240&host_id=ngasbe03:7777&quick_location=1&file_version=1&file_id=backup.2011-02-02T22:01:59.tar

    # Get file ID
    file_version = request_properties.fileinfo['fileVersion']
    file_id = request_properties.fileinfo['fileId']

    # Specific treatment depending on the mime-type
    if file_format.find("multipart") >= 0 or file_format.find("multialma") >= 0:
        logger.debug("Applying plug-in specific treatment")
        with open(staging_filename, "r") as fo:
            file_id, final_name, file_format = specific_treatment(fo)
    else:
        final_name = file_id

    logger.debug("File with URI %s is being handled by ngamsGeneric: file_format=%s, file_id=%s, file_version=%s," 
                 " final_name=%s", request_properties.getFileUri(), file_format, file_id, file_version, final_name)

    try:
        # Compression parameters
        uncompressed_size = ngamsPlugInApi.getFileSize(staging_filename)
        compression = ""

        # Filename and paths
        date = toiso8601(fmt=FMT_DATE_ONLY)
        relative_path, relative_filename, complete_filename, file_exists = \
            generate_file_info(ngams_server.getCfg(), disk_info, staging_filename, file_version, final_name, [date])

        # Make sure the format is defined
        if not file_format:
            file_format = ngamsPlugInApi.determineMimeType(ngams_server.getCfg(), staging_filename)

        file_size = ngamsPlugInApi.getFileSize(staging_filename)

        return ngamsPlugInApi.genDapiSuccessStat(disk_info.getDiskId(), relative_filename, file_id, file_version,
                                                 file_format, file_size, uncompressed_size, compression, relative_path,
                                                 disk_info.getSlotId(), file_exists, complete_filename)
    except Exception as e:
        error_message = "Problem processing file in staging area: " + str(e)
        exception_message = genLog("NGAMS_ER_DAPI_BAD_FILE", [staging_filename, _PLUGIN_ID, error_message])
        raise Exception(exception_message)
