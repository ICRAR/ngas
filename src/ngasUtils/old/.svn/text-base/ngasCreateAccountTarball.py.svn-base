

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

#******************************************************************************
#
# "@(#) $Id: ngasCreateAccountTarball.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  16/07/2003  Created
#

"""
This script is used to create a tarball with the files which are needed
under the user NGAS user accounts (ngasmgr and ngas).

It should be invoked with the name of the user. The tarball generated
will be located under the name
'/opsw/packages/ngasSys/common/<User ID>-account.tar'.
"""

import os, sys, commands


def execCmd(cmd):
    """
    Execute a shell command.

    cmd:      Command to execute on the shell (string).

    Returns:  Command status + output on stdout/stderr (tuple).
    """
    print "===> Executing command: " + cmd
    return commands.getoutput(cmd)


def createAccountTarballs(user):
    """
    Create a tarball with the files needed for the NGAS users accounts
    (ngas and ngasmgr).

    user:      User ID (string).

    Returns:   Void.
    """
    homeDir = os.path.expanduser("~" + user) + "/"

    print "=> Create tarball of the user account: " + user
    tarball = "/tmp/" + user + "-account.tar"
    execCmd("rm -f " + tarball)

    cmd = "cd " + homeDir + "/..; tar cf " + tarball
    files = [".bash_logout", ".bash_profile", ".bashrc", ".emacs",
             ".emacs-modes"]
    for file in files:
        tmpPath = os.path.normpath(user + "/" + file)
        cmd += " " + tmpPath
    execCmd(cmd)
    targTarball = "/opsw/packages/ngasSys/common/" + user + "-account.tar"
    execCmd("mv " + tarball + " " + targTarball)
    print "=> Account tarball can be found as: " + targTarball


if __name__ == '__main__':
    """
    Main program.
    """
    
    if (len(sys.argv) != 2):
        print "Correct usage is: \n"
        print "% ngasCreateAccountTarball.py <User ID>\n"
        sys.exit()
    user = sys.argv[1]
    createAccountTarballs(user)


# EOF
