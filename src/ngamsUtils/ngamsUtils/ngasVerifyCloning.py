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
# "@(#) $Id: ngasVerifyCloning.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  19/02/2003  Created
#

"""
Contains function to verify that a cloning carried has in fact cloned
all files so that they are available on the target disks/host.

NOTE: TO BE EXECUTED ON THE HOST ON WHICH THE FILES WHERE CLONED (TARGET
      HOST, PRESENCE OF FILES CHECKED).
"""

import logging
import os
import sys
import traceback

from ngamsLib.ngamsCore import getHostName
from ngamsLib import ngamsDiskInfo
from ngamsLib import ngamsFileInfo
from ngamsPClient import ngamsPClient
from . import ngasUtilsLib

LOGGING_FORMAT = "%(asctime)s %(processName)-20.20s %(levelname)-8.8s - %(message)s"
LOGGING_FILE_PATH = os.path.join(os.getcwd(), "ngas-verify-cloning.log")
logging.basicConfig(filename=LOGGING_FILE_PATH, format=LOGGING_FORMAT, level="DEBUG")
logging.getLogger(__name__).addHandler(logging.StreamHandler())
logger = logging.getLogger(__name__)

# SQL values indices
INDEX_SLOT_ID = 0
INDEX_MOUNT_POINT = 1
INDEX_FILE_NAME = 2
INDEX_CHECKSUM = 3
INDEX_CHECKSUM_PLUGIN = 4
INDEX_FILE_ID = 5
INDEX_FILE_VERSION = 6
INDEX_FILE_SIZE = 7
INDEX_FILE_STATUS = 8
INDEX_DISK_ID = 9


def _get_target_host():
    """
    Get DB name for the target host where the auto clone was executed

    :return: DB reference for target host (string)
    """
    if "_NGAS_VERIFY_CLONING_TARGET_HOST_" in os.environ:
        target_host = os.environ["_NGAS_VERIFY_CLONING_TARGET_HOST_"]
    else:
        target_host = getHostName()
    return target_host


def _get_file_info1(connection, disk_id):
    """
    Retrieve information about files supposedly cloned

    :param connection: NG/AMS DB object (ngamsDb)
    :param disk_id: ID of disk to check (string)
    :return: NG/AMS Cursor Object (ngamsDbCursor)
    """
    logger.info("Entering _get_file_info1() ...")

    sql_format = "select nd.slot_id, nd.mount_point, nf.file_name, nf.checksum, nf.checksum_plugin, nf.file_id, " \
                 "nf.file_version, nf.file_size, nf.file_status, nd.disk_id " \
                 "from ngas_disks nd, ngas_files nf " \
                 "where nf.disk_id='{:s}' and nd.disk_id='{:s}'"
    sql = sql_format.format(disk_id, disk_id)

    with connection.dbCursor(sql) as cursor:
        for result in cursor.fetch(1000):
            yield result

def _get_file_info2(connection, host_id, source_disk_id):
    """
    Get information about all files stored on the given host

    :param connection: NG/AMS DB Object (ngamsDb)
    :param host_id: ID of host for which to query files (string)
    :param source_disk_id: Source disk (i.e. disk that was cloned) (string)
    :return: NG/AMS Cursor Object (ngamsDbCursor)
    """
    logger.info("Entering _get_file_info2() ...")

    sql_format = "select nd.slot_id, nd.mount_point, nf.file_name, nf.checksum, nf.checksum_plugin, nf.file_id, " \
                 "nf.file_version, nf.file_size, nf.file_status, nd.disk_id " \
                 "from ngas_disks nd, ngas_files nf " \
                 "where nd.host_id='{:s}' " \
                 "and nf.disk_id=nd.disk_id and nf.disk_id!='{:s}'"
    sql = sql_format.format(host_id, source_disk_id)

    with connection.dbCursor(sql) as cursor:
        for result in cursor.fetch(1000):
            yield result

def clone_check_file(disk_id, file_id, file_version, connection, ngas_client):
    """
    Clone a file a check afterwards if it was successfully cloned

    :param disk_id: ID of disk cloned (string)
    :param file_id: ID of file cloned (string)
    :param file_version: Version of file to check (integer)
    :param connection: DB connection object (ngamsDb)
    :param ngas_client: Initiated instance of NG/AMS P-Client Object (ngamsPClient)
    :return: Updated Check Report (string)
    """
    message = "\n-> Attempting to clone file: {:s}/{:s}/{:s}".format(disk_id, file_id, file_version)
    print(message)
    message += " - status: "
    result = ngas_client.clone(file_id, disk_id, file_version, wait=1)
    if result.getStatus() == "FAILURE":
        status = "FAILURE: " + str(result.get_message()) + "\n"
    else:
        # Check if file was really cloned
        result = connection.getFileInfoFromFileIdHostId(_get_target_host(), file_id, file_version)
        if not result:
            status = "FAILURE: File not cloned!\n"
        else:
            file_info = ngamsFileInfo.ngamsFileInfo().unpackSqlResult(result)
            file_status_parameters = [["disk_id", file_info.get_disk_id()], ["file_access", file_id],
                                      ["file_version", file_version]]
            client = ngamsPClient.ngamsPClient(getHostName(), ngas_client.getPort())
            result = client.status(file_status_parameters)
            status = result.get_message() + "\n"
    return message + status


def check_cloning(source_disk_id, auto_clone):
    """
    Function to check if all files registered for a given disk, have been cloned onto disks in the system where this
    tool is executed.

    :param source_disk_id: ID of the cloned disk (string)
    :param auto_clone: If a file is not cloned will be automatically cloned by the tool (integer/0|1)
    :return: Status message (string)
    """
    if auto_clone:
        port_num = ngasUtilsLib.get_parameter_ngas_resource_file("NgasPort")
        ngams_client = ngamsPClient.ngamsPClient(getHostName(), port_num)
    else:
        ngams_client = None

    check_report_main = 50 * "=" + "\n"
    check_report_main += "Clone Verification Report - Disk: {:s}\n\n".format(source_disk_id)
    check_report = ""

    connection = ngasUtilsLib.get_db_connection()

    # Get information about files on the Source Disk
    disk_info = ngamsDiskInfo.ngamsDiskInfo()
    disk_info = disk_info.read(connection, source_disk_id)
    if not disk_info:
        raise Exception("Specified Disk ID: {:s} is unknown. Aborting.".format(source_disk_id))
    if disk_info.getHostId().strip() == getHostName():
        msg = "Source disk specified Disk ID: {:s} is inserted in this node: {:s}"
        raise Exception(msg.format(source_disk_id, getHostName()))

    file_info_list = _get_file_info1(connection, source_disk_id)
    source_file_list = []
    for file_info in file_info_list:
        source_file_list.append(file_info)
    del file_info_list

    # Get information about files on the target host (.e.g. this host)
    # NGAS Utils Functional Tests: Use special target host if requested
    file_info_list = _get_file_info2(connection, _get_target_host(), source_disk_id)
    target_file_list = []
    for file_info in file_info_list:
        target_file_list.append(file_info)
    del file_info_list

    # Build a dictionary with the target files with (<File ID>, <File Version>) as keys
    target_file_dict = {}
    for file_info in target_file_list:
        target_file_dict[(file_info[INDEX_FILE_ID], file_info[INDEX_FILE_VERSION])] = file_info

    # Now go through each source file and check if it is registered in the DB among the target files
    for file_info in source_file_list:
        source_file_id = file_info[INDEX_FILE_ID]
        source_file_version = file_info[INDEX_FILE_VERSION]
        key = (source_file_id, source_file_version)

        # Check if target file is present in the DB
        if key not in target_file_dict:
            check_report += "*** Missing target file in DB: {:s}\n".format(file_info)
            if auto_clone:
                check_report += clone_check_file(file_info[INDEX_DISK_ID], file_info[INDEX_FILE_ID],
                                                 file_info[INDEX_FILE_VERSION], connection, ngams_client)
                check_report += "\n"
            continue

        target_file_info = target_file_dict[key]
        mount_point = target_file_info[INDEX_MOUNT_POINT]
        filename = target_file_info[INDEX_FILE_NAME]
        target_file_path = os.path.normpath(os.path.join(mount_point, filename))
        message = "*** Checking file: {:s}".format(target_file_path)
        if auto_clone:
            message = "\n" + message
        print(message)

        # 1. Check that the target file is physically present on the target disk
        if not os.path.exists(target_file_path):
            check_report += "Missing target file on disk: {:s}\n".format(str(file_info))
            if auto_clone:
                check_report += clone_check_file(file_info[INDEX_DISK_ID], file_info[INDEX_FILE_ID],
                                                 file_info[INDEX_FILE_VERSION], connection, ngams_client)
                check_report += "\n"
            continue

        # 2. Check that the size is correct
        source_file_size = file_info[INDEX_FILE_SIZE]
        target_file_size = os.path.getsize(target_file_path)
        if source_file_size != target_file_size:
            check_report += "Wrong size of target file: {:s}\n".format(str(file_info))
            check_report += " - Check file manually!\n"

    if check_report:
        check_report_main += check_report
    else:
        check_report_main += "No discrepancies found\n"

    return check_report_main


def correct_usage():
    """
    Print correct usage of tool to stdout
    """
    print("\nCorrect usage is:\n")
    print("> python ngasVerifyCloning.py -diskId \"<Disk ID>[,<Disk ID>]\""
          " [-autoClone] [-notifEmail <Email Rec List>]\n")


def main():
    """
    Main function
    """
    if len(sys.argv) < 2:
        correct_usage()
        sys.exit(1)

    # Parse input parameters
    disk_id = ""
    auto_clone = 0
    notification_email = None
    idx = 1
    while idx < len(sys.argv):
        par = sys.argv[idx].upper()
        try:
            if par == "-DISKID":
                idx += 1
                disk_id = sys.argv[idx]
            elif par == "-AUTOCLONE":
                auto_clone = 1
            elif par == "-NOTIFEMAIL":
                idx += 1
                notification_email = sys.argv[idx]
            else:
                sys.exit(1)
            idx += 1
        except Exception as e:
            print("\nProblem initializing Clone Verification Tool: {:s}\n".format(str(e)))
            correct_usage()
            sys.exit(1)

    if not disk_id:
        correct_usage()
        sys.exit(1)

    try:
        if notification_email is None:
            notification_email = ngasUtilsLib.get_parameter_ngas_resource_file(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)

        # Execute the cloning
        disk_id_list = disk_id.split(",")
        print(50 * "=")
        for source_disk_id in disk_id_list:
            report = check_cloning(source_disk_id, auto_clone)
            if auto_clone:
                report += "\n\nNOTE: Files might have been cloned - please re-run the tool without the " \
                          "-autoClone option\n\n"
            print(report)
            print(50 * "=")

            # Send the email notification report
            ngasUtilsLib.send_email("Cloning Verification Report for disk: {:s}".format(source_disk_id),
                                    notification_email, report, "text/plain",
                                    "CLONE_VER_REP_{:s}".format(source_disk_id))
    except Exception as e:
        print("ERROR occurred executing the Clone Verification Tool: \n\n" + str(e) + "\n")
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
