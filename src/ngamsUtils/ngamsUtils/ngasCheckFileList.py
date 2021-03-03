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
# "@(#) $Id: ngasCheckFileList.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  19/01/2004  Created
#

"""
Utility to check the consistency of a list of files.

A File List has the following contents:

  <Disk ID> <File ID> <File Version>
  <Disk ID> <File ID> <File Version>
  ...

It is also possible to use a DCC Report as input file list. The tool will
then generate a file list internally from this.
"""

import logging
import os
import sys

from ngamsLib.ngamsCore import NGAMS_CHECKFILE_CMD, NGAMS_FAILURE, NGAMS_SUCCESS
from ngamsPClient import ngamsPClient
import ngasUtilsLib

LOGGING_FORMAT = "%(asctime)s %(processName)-20.20s %(levelname)-8.8s - %(message)s"
LOGGING_FILE_PATH = os.path.join(os.getcwd(), "ngas-check-file-list.log")
logging.basicConfig(filename=LOGGING_FILE_PATH, format=LOGGING_FORMAT, level="DEBUG")
logging.getLogger(__name__).addHandler(logging.StreamHandler())
logger = logging.getLogger(__name__)


def check_file_list(host, port, file_list_file, notification_email, ignore_disk_id):
    """
    Check if each file in the list is accessible by sending a CHECKFILE Command to the specified NG/AMS Server

    :param host: Host name of remote NG/AMS Server (string)
    :param port: Port number used by remote NG/AMS Server (integer).
    :param file_list_file: File containing list of Files IDs for files to check (string)
    :param notification_email: Comma separated list of email recipients that should be informed about the actions
    carried out, or the actions that would be carried out if executing the command (integer/0|1)
    :param ignore_disk_id: Ignore the Disk ID of the file, just check that one such file is available (integer/0|1)
    """
    file_reference_list = ngasUtilsLib.parse_file_list(file_list_file)
    report = 50 * "=" + "\n"
    report += "FILE CHECK REPORT:\n"
    report += 50 * "-" + "\n"
    print("\n" + report)
    for file_info in file_reference_list:
        disk_id = file_info[0]
        file_id = file_info[1]
        file_version = file_info[2]
        if ignore_disk_id:
            msg = "Checking file: {:s}/{:s} - Status: ".format(str(file_id), str(file_version))
        else:
            msg = "Checking file: {:s}/{:s}/{:s} - Status: ".format(str(disk_id), str(file_id), str(file_version))
        print(msg)
        # parameters = [["file_access", file_id], ["file_version", file_version]]
        parameters = [["file_id", file_id], ["file_version", file_version]]
        if not ignore_disk_id:
            parameters.append(["disk_id", disk_id])
        client = ngamsPClient.ngamsPClient(host, port)
        # result = client.status(parameters)
        # if result.getMessage().find("NGAMS_INFO_FILE_AVAIL") == -1:
        result = client.get_status(NGAMS_CHECKFILE_CMD, pars=parameters)
        if result.getMessage().find("FILE_OK") == -1:
            status = NGAMS_FAILURE + "/" + result.getMessage()
        else:
            status = NGAMS_SUCCESS + "/OK"
        print(status + "\n")
        msg += status
        report += msg + "\n"
    report += 50 * "-" + "\n"
    print(50 * "-" + "\n")
    if notification_email:
        ngasUtilsLib.send_email("ngasCheckFileList: FILE CHECK REPORT", notification_email, report)


def correct_usage():
    """
    Generate help description

    :return: Help description (string)
    """
    message = "\nCorrect usage is:\n\n"
    message += "> ngasCheckFileList.py [-host <Host>] [-port <Port>] " +\
               "-fileList <FileList File>|<DCC Report (ASCII)> " +\
               "[-notifEmail <Email List>]\n" +\
               "[-ignoreDiskId]\n"
    return message


def main():
    """
    Main function to invoked the tool
    """
    file_list_file = ""
    host = ""
    port = 0
    notification_email = ""
    ignore_disk_id = 0
    index = 1
    while index < len(sys.argv):
        parameter = sys.argv[index].upper()
        if parameter == "-FILELIST":
            index += 1
            file_list_file = sys.argv[index]
        elif parameter == "-HOST":
            index += 1
            host = sys.argv[index]
        elif parameter == "-PORT":
            index += 1
            port = int(sys.argv[index])
        elif parameter == "-NOTIFEMAIL":
            index += 1
            notification_email = sys.argv[index]
        elif parameter == "-IGNOREDISKID":
            ignore_disk_id = 1
        else:
            correct_usage()
            sys.exit(1)
        index += 1

    if not notification_email:
        notification_email = ngasUtilsLib.get_parameter_ngas_resource_file(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
    if host == "":
        host = ngasUtilsLib.get_parameter_ngas_resource_file(ngasUtilsLib.NGAS_RC_PAR_HOST)
    if port == 0:
        port = int(ngasUtilsLib.get_parameter_ngas_resource_file(ngasUtilsLib.NGAS_RC_PAR_PORT))

    try:
        if not file_list_file:
            print(correct_usage())
            raise Exception("Incorrect command line parameter(s) given!")
        check_file_list(host, port, file_list_file, notification_email, ignore_disk_id)
    except Exception as e:
        print("\nProblem encountered:\n\n" + str(e) + " -- bailing out\n")


if __name__ == '__main__':
    main()
