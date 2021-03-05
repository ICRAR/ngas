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
# "@(#) $Id: ngasUtilsLib.py,v 1.4 2008/12/15 22:09:52 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  21/01/2004  Created
#
"""
Utility functions used by the tool in the NGAS Utils module
"""

import base64
import getpass
import glob
import logging
import os
import shutil
import smtplib
import time

from ngamsLib.ngamsCore import getHostName, getFileCreationTime, NGAMS_NOT_RUN_STATE, ngamsCopyrightString
from ngamsLib import ngamsDb
from ngamsPClient import ngamsPClient

logger = logging.getLogger(__name__)

NGAS_RC_FILE = "~/.ngas"

NGAS_RC_PAR_ACC_CODE = "AccessCode"
NGAS_RC_PAR_DB_INT = "DbInterface"
NGAS_RC_PAR_DB_SRV = "DbServer"
NGAS_RC_PAR_DB_USER = "DbUser"
NGAS_RC_PAR_DB_PWD = "DbPassword"
NGAS_RC_PAR_DB_NAME = "DbName"
NGAS_RC_PAR_SMTP_HOST = "SmtpHost"
NGAS_RC_PAR_NOTIF_EMAIL = "EmailNotification"
NGAS_RC_PAR_HOST = "NgasHost"
NGAS_RC_PAR_PORT = "NgasPort"


def get_ngas_resource_file():
    """
    Get the complete name of the NGAS resource file

    :return: Complete filename of NGAS resource file (string)
    """
    return os.path.expanduser(NGAS_RC_FILE)


def get_parameter_ngas_resource_file(parameter):
    """
    Retrieve a parameter from the NGAS resource file.

    The function has a built-in check to ensure that it is not possible to execute the tool on a test machine. A test
    machine has one of the following substrings in its name: 'wg0ngas', 'dev', 'tst'.

    :param parameter: Parameter name (string)
    :return: Value of parameter or None (string|None)
    """
    # Determine if running on a test system
    test_system_list = ['wg0ngas', 'dev', 'tst']
    test_system = 0
    for test_system_pattern in test_system_list:
        if getHostName().find(test_system_pattern) != -1:
            test_system = 1
    try:
        with open(get_ngas_resource_file()) as fo:
            file_lines = fo.readlines()
    except Exception as e:
        raise Exception("Error accessing NGAS Resource File: {:s}".format(str(e)))

    for line in file_lines:
        try:
            value = line[line.find("=") + 1:].strip()
        except Exception as e:
            message = "Problem parsing line |{:s}| from NGAS Utilities Resource File. Error: {:s}"
            raise Exception(message.format(line, str(e)))
        # Some basic on-the-fly checks:
        #   1. Only allow to execute with ESOECF/OLASLS/ASTOP if not test system.
        #   2. Do not allow to send Email Notification to email address containing the substrings 'sao' and 'ngasgop'.
        #   3. Only allow to execute on system with hostname containing substring mentioned in the function man-page.
        if test_system:
            if line.find("DbServer") != -1:
                if value == "ESOECF" or value == "OLASLS" or value == "ASTOP":
                    raise Exception("Cannot connect to operational DB: {:s}".format(value))
            elif line.find("EmailNotification") != -1:
                if value.find("ngasgop") != -1 or value.find("sao") != -1:
                    raise Exception("Cannot send Email Notification Messages to operators: {:s}".format(value))
            elif line.find("NgasHost") != -1:
                if value.find("wg0ngas") == -1 and value.find("dev") == -1 \
                        and value.find("tst") == -1 and value != "$HOSTNAME":
                    raise Exception("Cannot connect to operational NGAS System: {:s}".format(value))
        if line.find(parameter) != -1:
            # Resolve a possible environment variable
            if value[0] == "$":
                value = os.environ[value[1:]]
            return value
    return None


def encrypt_access_code(access_code):
    """
    Encode an Access Code as used by the NGAS Utilities

    :param access_code: Access Code as typed by the user (string)
    :return: Encoded Access Code (string)
    """
    return base64.encodestring(access_code)


def decrypt_access_code(encrypted_access_code):
    """
    Decode an Access Code as used by the NGAS Utilities

    :param encrypted_access_code: Encrypted Access Code (string)
    :return: Decoded Access Code (string)
    """
    return base64.decodestring(encrypted_access_code)


def check_access_code(access_code):
    """
    Check the given access code against the one defined in the NGAS resource file. In case of match, it returns 1
    otherwise 0.

    :param access_code: Access Code as given by the user (string)
    """
    decrypted_access_code = decrypt_access_code(get_parameter_ngas_resource_file(NGAS_RC_PAR_ACC_CODE))
    if decrypted_access_code == access_code:
        return
    else:
        raise Exception("Incorrect access code given!!")


def console_input(message):
    """
    Simple function to prompt the user for input. The string entered is stripped before returning it to the user.

    :param message: Message to print (string)
    :return: Information entered by the user (string)
    """
    return raw_input("INPUT> " + message + " ").strip()


def get_db_parameters():
    """
    Extract the DB parameters from the NGAS resource file. The DB password is decrypted.

    :return: Tuple with the DB parameters (<DB Interface>, <DB Srv>, <DB>, <User>, <Pwd>) (tuple)
    """
    interface = get_parameter_ngas_resource_file(NGAS_RC_PAR_DB_INT)
    server = get_parameter_ngas_resource_file(NGAS_RC_PAR_DB_SRV)
    db = get_parameter_ngas_resource_file(NGAS_RC_PAR_DB_NAME)
    user = get_parameter_ngas_resource_file(NGAS_RC_PAR_DB_USER)
    password = get_parameter_ngas_resource_file(NGAS_RC_PAR_DB_PWD)
    return interface, server, db, user, password


def send_email(subject, to, message, content_type=None, attachment_name=None):
    """
    Send an e-mail to the recipient with the given subject

    :param subject: Subject of mail message (string)
    :param to: Recipient (e.g. user@exmaple.com) (string)
    :param message: Message to send (string).
    :param content_type: Mime-type of message (string)
    :param attachment_name: Name of attachment in mail (string)
    """
    smtp_host = get_parameter_ngas_resource_file(NGAS_RC_PAR_SMTP_HOST)
    email_list = to.split(",")
    from_field = getpass.getuser() + "@" + os.uname()[1].split(".")[0]
    for emailAdr in email_list:
        try:
            hdr = "Subject: " + subject + "\n"
            if content_type:
                hdr += "Content-Type: " + content_type + "\n"
            if attachment_name:
                hdr += "Content-Disposition: attachment; filename=" + attachment_name + "\n"
            tmp_message = hdr + "\n" + message
            server = smtplib.SMTP(smtp_host)
            server.sendmail("From: " + from_field, "Bcc: " + emailAdr, tmp_message)
        except Exception as e:
            print("Error sending email to recipient: " + str(emailAdr) + ". Error: " + str(e))


def dcc_message_to_file_list(dcc_message_file, target_file):
    """
    Converts a DCC (Data Consistency Checking, Inconsistency Notification
    Message, e.g.:

    Notification Message:
    ...
    Problem Description                      File ID                       ...
    -----------------------------------------------------------------------...
    ERROR: File in DB missing on disk        WFI.1999-07-07T21:01:45.296   ...
    ERROR: File in DB missing on disk        WFI.1999-07-07T21:00:37.187   ...
    ...
    -----------------------------------------------------------------------...

    - into a File List of the format:

    <Disk ID> <File ID> <File Version>
    ...

    Can also convert a DCC message of the form:

    Notification Message:
    ...
    ------------------------------------------------------------------------...
    NON REGISTERED FILES FOUND ON STORAGE DISKS:

    Filename:
    ------------------------------------------------------------------------...
    /tmp/ngamsTest/NGAS/FitsStorage1-Main-1/ShouldNotBeHere1
    /tmp/ngamsTest/NGAS/FitsStorage1-Main-1/.db/ShouldNotBeHere2
    ...
    ------------------------------------------------------------------------...

    - into a File List of the format:

    <Complete Path 1>
    <Complete Path 2>
    ...

    :param dcc_message_file: File containing the DCC inconsistent message (string)
    :param target_file: Target file
    """
    with open(dcc_message_file) as fo:
        dcc_message = fo.read()
        dcc_message_lines = dcc_message.split("\n")
    fo = open(target_file, "w")
    if dcc_message.find("NON REGISTERED FILES FOUND ON STORAGE DISK") != -1:
        file_list_buffer = ""
        # Skip until line starting with '/' found
        total_lines = len(dcc_message_lines)
        line_num = 0
        while line_num < total_lines:
            line = dcc_message_lines[line_num].strip()
            if line == "":
                line_num += 1
                continue
            if line[0] == "/":
                break
            line_num += 1
        while line_num < total_lines:
            line = dcc_message_lines[line_num].strip()
            if line[0] == "-":
                break
            file_list_buffer += "{:s}\n".format(line)
            line_num += 1
        fo.write(file_list_buffer)
    else:
        line_num = 0
        while 1:
            line = dcc_message_lines[line_num]
            if line.find("Problem Description") != -1:
                file_id_index = line.find("File ID")
                break
            line_num += 1
        line_num += 2

        # Read in lines until "-----..." is encountered
        while 1:
            if dcc_message_lines[line_num].find("-----") != -1:
                break
            line_elements = dcc_message_lines[line_num][file_id_index:].split(" ")
            # element_list = cleanList(line_elements)
            element_list = filter(None, line_elements)
            file_id = element_list[0]
            file_version = element_list[1]
            disk_id = element_list[2].split(":")[1]
            fo.write("{:s} {:s} {:s}\n".format(disk_id, file_id, file_version))
            line_num += 1
    fo.close()


def dcc_report_to_file_list(dcc_report):
    """
    Converts a DCC Report to a File List

    :param dcc_report: DCC Report to convert (string)
    :return: Corresponding File list (string).
    """
    # IMPL: Note: This function should replace dccMsg2FileList().
    if dcc_report.find("DATA CHECK REPORT") == -1:
        raise Exception("The specified text appears not to be a DCC Report")
    file_list_buffer = ""
    for line in dcc_report.split("\n"):
        if line.find("ERROR: File in DB missing on disk") != -1 \
                or line.find("ERROR: Inconsistent checksum found") != -1:
            # clean_list = cleanList(line_elements)
            clean_list = filter(None, line.split(" "))
            disk_id = clean_list[-1].split(":")[-1]
            file_id = clean_list[-3]
            file_version = clean_list[-2]
            file_list_buffer += "\n{:s} {:s} {:s}".format(disk_id, file_id, file_version)
    return file_list_buffer


def parse_file_list(file_list_file):
    """
    Function that parses the the given file list listing entries like this:

       <Field 1> <Field 2> <Field 3>, ...
       <Field 1> <Field 2> <Field 3>, ...
       ...

    and returns the entries in a list:

      [[<Field 1>, <Field 2>, <Field 3>, ...], ...]

    If the File List input file specified contains a DCC Report, this will be handled as well.

    :param file_list_file: Name of file in which the file entries are contained (string)
    :return: List containing the info for the files to register (list).
    """
    file_reference_list = []
    with open(file_list_file) as fo:
        file_buffer = fo.read()

    # Check if the File List should be derived from a DCC Report
    if file_buffer.find("DATA CHECK REPORT") != -1:
        file_buffer = dcc_report_to_file_list(file_buffer)

    # Parse the file list and create a Python list with the info.
    file_lines = file_buffer.split("\n")
    line_num = 0
    for line in file_lines:
        line_num += 1
        line = line.strip()
        if not line:
            continue
        if line[0] == "#":
            continue
        fields = []
        element_list = line.split(" ")
        for element in element_list:
            if element.strip():
                fields.append(element.strip())
        file_reference_list.append(fields)
    return file_reference_list


def remove_recursively(path):
    """
    Remove files, links and directories recursively for a given path

    :param path: File system path
    """
    if not os.path.exists(path):
        return
    if os.path.isfile(path):
        os.remove(path)
    elif os.path.islink(path):
        os.unlink(path)
    else:
        shutil.rmtree(path)


def check_delete_tmp_directories(directory_name_pattern, time_out=600):
    """
    Check if there are temporary directories files according to the given pattern, which are older than the time out
    specified. In case yes, remove these if possible.

    :param directory_name_pattern: Pattern to check (string)
    :param time_out: Timeout in seconds, if entries older than this are found, they are deleted (integer)
    """
    glob_file_list = glob.glob(directory_name_pattern)
    for glob_file in glob_file_list:
        last_path = glob_file.split("/")[-1]
        if last_path != "." and last_path != "..":
            creation_time = getFileCreationTime(glob_file)
            if (time.time() - creation_time) > time_out:
                remove_recursively(glob_file)


def check_server_running(host=None, port=8001):
    """
    Function to check if server is running. It returns the status of the server, which is one of the following values:

      - NOT-RUNNING: Server is not running
      - OFFLINE:     Server is running and is in OFFLINE State
      - ONLINE:      Server is running and is in ONLINE State

    :param host: Host to check if server is running (string)
    :param port: Port number used by server (integer)
    :return: Status (string)
    """
    if host is None:
        host = getHostName()
    try:
        client = ngamsPClient.ngamsPClient(host, port)
        result = client.status()
        state = result.getState()
    except Exception:
        state = NGAMS_NOT_RUN_STATE
    return state


# Tools to handle command line options
# ------------------------------------

# IMPL: Use a class (ngasOptions) to handle the options rather than a dictionary
NGAS_OPT_NAME = 0
NGAS_OPT_ALT = NGAS_OPT_NAME + 1
NGAS_OPT_VAL = NGAS_OPT_ALT + 1
NGAS_OPT_TYPE = NGAS_OPT_VAL + 1
NGAS_OPT_SYN = NGAS_OPT_TYPE + 1
NGAS_OPT_DOC = NGAS_OPT_SYN + 1

NGAS_OPT_OPT = "OPTIONAL"
NGAS_OPT_MAN = "MANDATORY"
NGAS_OPT_INT = "INTERNAL"

_stdOptions = [["help", [], 0, NGAS_OPT_OPT, "",
                "Print out man-page and exit."],
               ["debug", [], 0, NGAS_OPT_OPT, "",
                "Run in debug mode."],
               ["version", [], 0, NGAS_OPT_OPT, "",
                "Print out version."],
               ["verbose", [], 0, NGAS_OPT_OPT, "=<Verbose Level [0; 5]>",
                "Switch verbose mode logging on ([0; 5])."],
               ["logFile", [], None, NGAS_OPT_OPT, "=<Log File>",
                "Log file into which info will be logged during execution."],
               ["logLevel", [], 0, NGAS_OPT_OPT, "=<Log Level [0; 5]>",
                "Level applied when logging into the specified log file."],
               ["notifEmail", [], None, NGAS_OPT_OPT, "=<Email Recep. List>",
                "Comma separated list of email recipients which will " +
                "receive status reports in connection with the tool execution."],
               ["accessCode", [], None, NGAS_OPT_OPT, "=<Access Code>",
                "Access code to access the NGAS system with the NGAS Utilities."]]


def generate_options_dictionary_and_document(tool_options):
    """
    From the options defined, generate a dictionary with this info and generate a man-page

    :param tool_options: Tool options (list)
    :return : Tuple with dictionary and man-page (tuple).
    """
    option_list = _stdOptions + tool_options
    option_dict = {}
    option_document = ""
    for option_info in option_list:
        option_dict[option_info[NGAS_OPT_NAME]] = option_info
        # FIXME: We store options twice in lower and upper case. This should not be necessary.
        option_dict[option_info[NGAS_OPT_NAME].upper()] = option_info
        if option_info[NGAS_OPT_NAME][0] != "_":
            option_document += "--{:s}{} [{:s}] ({:s}):\n".format(option_info[NGAS_OPT_NAME],
                                                                    option_info[NGAS_OPT_SYN],
                                                                    str(option_info[NGAS_OPT_VAL]),
                                                                    option_info[NGAS_OPT_TYPE])
            option_document += option_info[NGAS_OPT_DOC] + "\n\n"
    option_document += "\n" + ngamsCopyrightString()
    return option_dict, option_document


def parse_command_line(argv, option_dict):
    """
    Parse the command line parameters and pack the values into the Options Dictionary. Some basic checks are carried
    out.

    :param argv: List with arguments as contained in sys.argv (list)
    :param option_dict: Dictionary with information about options (dictionary)
    :return: Returns reference to updated Options Dictionary (dictionary)
    """
    # Parse input parameters
    index = 1
    while index < len(argv):
        logger.info("Parsing option: {:s}".format(argv[index]))
        tmp_info = argv[index].split("=")
        if len(tmp_info) >= 2:
            equal_index = argv[index].find("=")
            param_option = argv[index][0:equal_index].strip()[2:]
            param_value = argv[index][(equal_index + 1):].strip()
        else:
            param_option = tmp_info[0][2:]
            param_value = 1
        param_option_upper = param_option.upper()
        if param_option_upper not in option_dict:
            raise Exception("Unknown option: {:s}".format(param_option))
        option_dict[param_option_upper][NGAS_OPT_VAL] = param_value
        if param_option_upper.find("HELP") != -1:
            return option_dict
        index += 1

    # Check if mandatory options are all specified
    for option in option_dict.keys():
        if option.upper()[0] == option[0]:
            continue
        if option_dict[option][NGAS_OPT_TYPE] == NGAS_OPT_MAN and option_dict[option][NGAS_OPT_VAL] is None:
            raise Exception("Undefined, mandatory option: \"{:s}\"".format(option))
    return option_dict


def get_check_access_code(option_dict):
    """
    Check the access code according to the value found for this parameter in the Options Dictionary. If not defined in
    the Options Dictionary, the user is prompted for the access code.

    :param option_dict: Dictionary containing info about options (dictionary)
    """
    access_code = option_dict["accessCode"][NGAS_OPT_VAL]
    if not access_code:
        access_code = console_input("Enter Access Code:")
    check_access_code(access_code)


def option_dictionary_to_parameter_dictionary(option_dict):
    """
    Convert an NGAS Utils option dictionary into a simple dictionary containing the options/parameters as keys and the
    associated, assigned values.

    :param option_dict: Dictionary containing info about options (dictionary)
    :return: Parameter dictionary (dictionary)
    """
    param_dict = {}
    for option in option_dict.keys():
        param_dict[option] = option_dict[option][NGAS_OPT_VAL]
    return param_dict


def get_server_list_from_string(server_string):
    """
    Parse NGAS server string (e.g. "ngas1:8001,ngas1:8002") and convert
    into a ngamsPClient server list (e.g. [("ngas1", 8001), ("ngas1", 8002)])

    :param server_string: NGAS server list (e.g. "ngas1:8001,ngas1:8002") (string)
    """
    client_server_list = []
    for server in server_string.split(","):
        server_split = server.split(":")
        client_server_list.append((server_split[0], int(server_split[1])))
    return client_server_list


def get_db_connection():
    """
    Open a database connection using property values read from the resource file
    """
    interface, server, db, user, password = get_db_parameters()
    password = base64.b64decode(password)
    params = {
        "dsn": db,
        "user": user,
        "password": password,
        "threaded": True
    }
    return ngamsDb.ngamsDb(interface, params)
