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
# "@(#) $Id: ngasCheckFileCopies.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  30/01/2004  Created
#

"""
The ngasCheckCopies Tool, checks how many copies of each file is found in the
system, which are registered on the referenced disk.

For each file found on the disk, the following output is generated:

<File ID> <File Version> <Total> <Good>
...

- whereby, Good Copies refer to copies marked as being OK in the DB.
"""

import base64
import logging
import os
import sys

from ngamsLib import ngamsDb
from ngamsLib import ngamsDbCore
from ngamsLib import ngamsLib
from . import ngasUtilsLib

LOGGING_FORMAT = "%(asctime)s %(processName)-20.20s %(levelname)-8.8s - %(message)s"
LOGGING_FILE_PATH = os.path.join(os.getcwd(), "ngas-check-file-copies.log")
logging.basicConfig(filename=LOGGING_FILE_PATH, format=LOGGING_FORMAT, level="DEBUG")
logging.getLogger(__name__).addHandler(logging.StreamHandler())
logger = logging.getLogger(__name__)


def check_copies(disk_id, notification_email):
    """
    Check the total number of copies of the files found on the referred disk
    in the NGAS DB. Generate a report indicating the number of copies found.

    :param disk_id: ID of disk for which to check files (string)
    :param notification_email: Comma separated list of recipients of the report generated (string)
    """
    interface, server, db, user, password = ngasUtilsLib.get_db_parameters()
    password = base64.b64decode(password)
    params = {
        "dsn": db,
        "user": user,
        "password": password,
        "threaded": True
    }
    connection = ngamsDb.ngamsDb(interface, params)

    # Get the information about the files on the referenced disk
    logger.info("Retrieving info about files on disk: %s ...", disk_id)
    disk_file_generator = connection.getFileSummary1(diskIds=[disk_id])

    # Get all files found in the system with the given File ID/File Version
    logger.info("Retrieving info about all File IDs/Versions in the system on disk: %s ...", disk_id)
    disk_file_list = []
    disk_count = 0
    for file_info in disk_file_generator:
        file_id = file_info[ngamsDbCore.SUM1_FILE_ID]
        disk_file_list.append(file_id)
        disk_count += 1
    print("")
    logger.info("Retrieved info about %d files on disk: %s", disk_count, disk_id)

    logger.info("Retrieving info about all File IDs/Versions in the system on disk: %s ...", disk_id)
    glob_file_list = []
    file_count = 0
    glob_file_generator = connection.getFileSummary1(fileIds=disk_file_list)
    for file_info in glob_file_generator:
        glob_file_list.append(file_info)
        file_count += 1
    print("")
    logger.info("Retrieved info about %d File IDs/Versions in the system on disk: %s", file_count, disk_id)


    # Now, go through this list, and generate a dictionary with File ID/File Version as keys
    glob_file_dict = {}
    for file_info in glob_file_list:
        file_key = ngamsLib.genFileKey(None, file_info[ngamsDbCore.SUM1_FILE_ID], file_info[ngamsDbCore.SUM1_VERSION])
        if file_key not in glob_file_dict:
            glob_file_dict[file_key] = []
        glob_file_dict[file_key].append(file_info)

    # Order the list according to (1) Number of copies and (2) Alphabetically
    file_key_list = glob_file_dict.keys()
    file_key_list.sort()
    sort_file_dict = {}
    for file_key in file_key_list:
        file_info_list = glob_file_dict[file_key]
        num_copies = len(file_info_list)
        if num_copies not in sort_file_dict:
            sort_file_dict[num_copies] = {}
        sort_file_dict[num_copies][file_key] = file_info_list

    # Go through the global file dictionary and check each File ID and File Version the requested information
    report = "FILE COPIES CHECK REPORT:\n\n"
    report += "Disk ID: " + disk_id + "\n\n"
    message_format = "{:60.60s} {:7.7s} {:5.5s} {:4.4s}\n"
    report += message_format.format("File ID", "Version", "Total", "Good")
    report += 50 * "-" + "\n"
    no_file_key_list = sort_file_dict.keys()
    no_file_key_list.sort()
    for no_file_key in no_file_key_list:
        no_file_key_dict = sort_file_dict[no_file_key]
        file_key_list = no_file_key_dict.keys()
        file_key_list.sort()
        for file_key in file_key_list:
            total_copies = 0
            good_copies = 0
            for file_info in no_file_key_dict[file_key]:
                total_copies += 1
                if file_info[ngamsDbCore.SUM1_FILE_STATUS][0] == "0" and file_info[ngamsDbCore.SUM1_FILE_IGNORE] == 0:
                    good_copies += 1
            file_id = file_info[ngamsDbCore.SUM1_FILE_ID]
            file_version = file_info[ngamsDbCore.SUM1_VERSION]
            report += message_format.format(file_id, str(file_version), str(total_copies), str(good_copies))

    if len(no_file_key_list):
        report += 50 * "-" + "\n\n"
    else:
        report += "No files found on the given disk!\n\n"
    print("\n" + report)

    if notification_email:
        ngasUtilsLib.send_email("ngasCheckFileCopies: FILE COPIES CHECK REPORT (%s)".format(disk_id),
                                notification_email, report)


def correct_usage():
    """
    Return the usage/online documentation in a string buffer

    :return: Help description (string)
    """
    message = "\nCorrect usage is:\n\n" +\
              "> ngasCheckFileCopies.py -diskId <Disk ID> " +\
              "[-notifEmail <Email List>]\n\n"
    return message


def main():
    # Parse input parameters
    disk_id = ""
    notification_email = None
    index = 1
    while index < len(sys.argv):
        parameter = sys.argv[index].upper()
        try:
            if parameter == "-DISKID":
                index += 1
                disk_id = sys.argv[index]
            elif parameter == "-NOTIFEMAIL":
                index += 1
                notification_email = sys.argv[index]
            elif parameter == "-VERBOSE":
                index += 1
            else:
                sys.exit(1)
            index += 1
        except Exception as e:
            print("\nProblem executing the tool: %s\n".format(str(e)))
            print(correct_usage())
            sys.exit(1)
    if notification_email is None:
        notification_email = ngasUtilsLib.get_parameter_ngas_resource_file(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
    try:
        if not disk_id:
            print(correct_usage())
            raise Exception("Incorrect command line parameter(s) given!")
        check_copies(disk_id, notification_email)
    except Exception as e:
        print("\nProblem encountered:\n\n" + str(e) + " -- bailing out\n")


if __name__ == '__main__':
    main()
