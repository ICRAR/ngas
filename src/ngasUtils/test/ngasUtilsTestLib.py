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
# "@(#) $Id: ngasUtilsTestLib.py,v 1.2 2008/08/19 20:37:46 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/08/2005  Created
#

"""
Common utilities for the Test Suite of the NGAS Utilities.
"""
import getpass, popen2

from ngams import *
import ngamsConfig
import ngamsLib
import ngamsTestLib


BASE_CFG_1      = "../../ngams/ngamsTest/src/ngamsCfg.xml"
TEST_FILE_1     = "../../ngams/ngamsTest/src/SmallFile.fits"
COMPL_HOST_NAME = ngamsLib.getCompleteHostName()


def prepNgasResFile(cfgFile,
                    accessCode="X190ZXN0X18=",
                    smtpHost="localhost",
                    emailNotification="%s@%s" % (getpass.getuser(),
                                                 COMPL_HOST_NAME),
                    ngasHost=getHostName(),
                    ngasPort=7777,
                    resFile="/home/%s/.ngas" % getpass.getuser()):
    """
    Prepare the NGAS Utilities resource file for the test (~/.ngas).

    cfgFile:              Configuration file relevant for the test (string).
    
    accessCode:           Access code (encrypted) used when authorizing
                          usage of the NGAS Utilities (string).
    
    smtpHost:             SMTP host used to send email notification (string).
    
    emailNotification:    Recipients of the email notification (string).
    
    ngasHost:             NGAS host on which the NG/AMS Server concerned is
                          running (string).
    
    ngasPort:             Port used by NG/AMS Server concerned (integer).

    Returns:              Void.
    """
    info(1,"Entering prepNgasResFile() ...")
    cfg = ngamsConfig.ngamsConfig().load(cfgFile)
    dbServer   = cfg.getDbServer()
    dbName     = cfg.getDbName()
    dbUser     = cfg.getDbUser()
    dbPassword = cfg.getDbPassword()
    fo = open("src/ngas")
    ngasRes = fo.read()
    fo.close()
    ngasRes = ngasRes % (accessCode, dbServer, dbUser, dbPassword, dbName,
                         smtpHost, emailNotification, ngasHost, ngasPort)
    rmFile(resFile)
    fo = open(resFile, "w")
    fo.write(ngasRes)
    fo.close()
    info(1,"Leaving prepNgasResFile()")


class ngasUtilsTestSuite(ngamsTestLib.ngamsTestSuite):
    """
    Test Case class for the NGAS Utilities Tests.
    """

    def __init__(self,
                 methodName = "runTest"):
        """
        Constructor method.
        """
        ngamsTestLib.ngamsTestSuite.__init__(self, methodName)
        

    def tearDown(self):
        """
        Tear down the test environment. Clean up if specified.

        Returns:   Void.
        """
        ngamsTestLib.ngamsTestSuite.tearDown(self)
        if (not ngamsTestLib.getNoCleanUp()):
            tmpDir = os.path.normpath(ngamsGetSrcDir() +\
                                      "/../ngasUtils/test/tmp")
            try:
                commands.getstatusoutput("mv -f %s/CVS %s/.CVS" %\
                                         (tmpDir, tmpDir))
                commands.getstatusoutput("rm -rf %s/*" % tmpDir)
                commands.getstatusoutput("mv -f %s/.CVS %s/CVS" %\
                                         (tmpDir, tmpDir))
            except:
                commands.getstatusoutput("mv -f %s/.CVS %s/CVS" %\
                                         (tmpDir, tmpDir))


# EOF
