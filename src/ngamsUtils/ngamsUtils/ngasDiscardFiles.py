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
# "@(#) $Id: ngasDiscardFiles.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  21/01/2004  Created
#


"""
Tool to remove files from the NGAS DB + Disk.

The disk(s) hosting he files to be removed/discarded from the NGAS Archive
must inserted in the NGAS System on which this command is executed.

The format of the file list is:

<Disk ID> <File ID> <File Version>
<Disk ID> <File ID> <File Version>
...

It is also possible to give a list of files, referred to by the complete
path, e.g.:

/NGAS/data7/saf/2001-06-11/41/WFI.2001-06-11T22:07:05.290.fits.Z
/NGAS/data7/saf/2001-06-11/36/WFI.2001-06-11T22:07:05.290.fits.Z
/NGAS/data7/saf/2001-06-11/93/WFI.2001-06-11T22:07:05.290.fits.Z
/NGAS/data7/saf/2001-06-11/86/WFI.2001-06-11T22:07:05.290.fits.Z
...

Note, in this case the tool will not attempt to remove the information
about the files from the DB. This should only be used to remove files
stored on disk, but not registered in the DB.


                          *** CAUTION ***

THIS IS A VERY DANGEROUS TOOL TO USE, SINCE IT ALLOWS TO REMOVE ARCHIVED
FILES FROM AN NGAS ARCHIVE ALSO IF THESE ARE AVAILABLE IN LESS THAN 3
COPIES. SHOULD BE USED WITH GREAT CAUTION!!!
"""

import logging
import os
import sys
import tempfile

from ngamsLib.ngamsCore import getHostName, rmFile, NGAMS_SUCCESS, NGAMS_DISCARD_CMD
from ngamsPClient import ngamsPClient
import ngasUtilsLib

logger = logging.getLogger(__name__)


def _unpack_file_info(file_list_line):
    """
    Unpack the information in the line read from the File List File.

    :param file_list_line:  Line as read from the file (string)
    :return: Tuple with file info: (<Disk ID>, <File ID>, <File Version>) (tuple)
    """
    file_info_list = []
    line_elements = file_list_line.split(" ")
    for element in line_elements:
        if element.strip() != "" and element.strip() != "\t":
            file_info_list.append(element.strip())
            if len(file_info_list) == 3:
                break
    if len(file_info_list) == 3:
        return file_info_list[0], file_info_list[1], file_info_list[2]
    else:
        raise Exception("Illegal line found in File List File: {:s}".format(file_list_line))


def discard_files(file_list_file, execute, notification_email):
    """
    Remove files from the NGAS DB + from from the disk. If files are given by their full path, only the file is
    removed from disk, but the DB information remains.

    :param file_list_file: Name of file containing list with references to files to remove (string)
    :param execute: Actual remove the files (integer/0|1)
    :param notification_email: List of email addresses to inform about the execution of the discard procedure (string)
    """
    host = ngasUtilsLib.get_parameter_ngas_resource_file(ngasUtilsLib.NGAS_RC_PAR_HOST)
    port = int(ngasUtilsLib.get_parameter_ngas_resource_file(ngasUtilsLib.NGAS_RC_PAR_PORT))
    # client = ngamsPClient.ngamsPClient(host, port, auth="ngas:password")
    client = ngamsPClient.ngamsPClient(host, port)

    # Try to issue a DISCARD Command to the associated NG/AMS Server for each file listed in the File List
    with open(file_list_file) as fo:
        file_list_buffer = fo.readlines()
    success_stat_list = []
    failed_stat_list = []
    for line in file_list_buffer:
        line = line.strip()
        if line == "" or line[0] == "#":
            continue
        if line[0] != "/":
            # It's a "<Disk ID> <File ID> <File Version>" line
            disk_id, file_id, file_version = _unpack_file_info(line)
            parameters = [["disk_id", disk_id], ["file_id", file_id],
                          ["file_version", file_version], ["execute", execute]]
        else:
            # The file is referred to by its complete path.
            parameters = [["path", line], ["execute", execute]]
        status = client.get_status(NGAMS_DISCARD_CMD, pars=parameters)
        if status.getStatus() == NGAMS_SUCCESS:
            success_stat_list.append((line, status))
        else:
            failed_stat_list.append((line, status))

    # Generate report (Email Notification)
    report = "FILE DISCARD REPORT:\n"
    report += "Host: {:s}".format(getHostName())
    if len(failed_stat_list):
        if execute:
            report += "\n\n=Failed File Discards:\n\n"
        else:
            report += "\n\n=Rejected File Discard Requests:\n\n"
        for statInfo in failed_stat_list:
            report += "{:s}: {:s}\n".format(statInfo[0], statInfo[1].getMessage())
    if len(success_stat_list):
        if execute:
            report += "\n\n=Discarded Files:\n\n"
        else:
            report += "\n\n=Accepted Discard File Requests:\n\n"
        for statInfo in success_stat_list:
            report += "{:s}: {:s}\n".format(statInfo[0], statInfo[1].getMessage())
    report += "\n# EOF\n"
    print "\n" + report
    if notification_email:
        ngasUtilsLib.send_email("ngasDiscardFiles: FILE DISCARD REPORT", notification_email, report)


def correct_usage():
    """
    Return the usage/online documentation in a string buffer

    :return: Help description (string)
    """
    message = "\nCorrect usage is:\n\n" +\
              "> python ngasDicardFiles.py [-accessCode <Code>] \n" +\
              "         -fileList <File List> | -dccMsg <DCC Msg File>\n" +\
              "         [-execute] [-notifEmail <Email List>]\n\n" +\
              __doc__ + "\n"
    return message


def main():
    """
    Main function to execute the tool
    """
    # Parse input parameters
    access_code = ""
    file_list_file = ""
    dcc_message_file = ""
    execute = 0
    notification_email = None
    index = 1
    while index < len(sys.argv):
        par = sys.argv[index].upper()
        try:
            if par == "-ACCESSCODE":
                index += 1
                access_code = sys.argv[index]
            elif par == "-FILELIST":
                index += 1
                file_list_file = sys.argv[index]
            elif par == "-DCCMSG":
                index += 1
                dcc_message_file = sys.argv[index]
            elif par == "-EXECUTE":
                execute = 1
            elif par == "-NOTIFEMAIL":
                index += 1
                notification_email = sys.argv[index]
            else:
                sys.exit(1)
            index += 1
        except Exception as e:
            print("\nProblem executing the File Discard Tool: {:s}\n".format(str(e)))
            print correct_usage()
            sys.exit(1)

    if notification_email is None:
        notification_email = ngasUtilsLib.get_parameter_ngas_resource_file(ngasUtilsLib.NGAS_RC_PAR_NOTIF_EMAIL)
    if dcc_message_file and not file_list_file:
        file_list_file = os.path.join(tempfile.gettempdir(), "ngasDiscardFiles.tmp")
        rmFile(file_list_file)
        ngasUtilsLib.dcc_message_to_file_list(dcc_message_file, file_list_file)
    try:
        if not file_list_file:
            print correct_usage()
            raise Exception("Incorrect command line parameter(s) given!")
        if not access_code:
            access_code = ngasUtilsLib.console_input("Enter Access Code:")
        ngasUtilsLib.check_access_code(access_code)
        discard_files(file_list_file, execute, notification_email)
    except Exception as e:
        print("\nProblem encountered:\n\n" + str(e) + " -- bailing out\n")


if __name__ == '__main__':
    main()
