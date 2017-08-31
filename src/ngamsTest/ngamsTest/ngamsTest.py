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
# "@(#) $Id: ngamsTest.py,v 1.7 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/04/2002  Created
#
"""
Test program that runs all the NG/AMS Unit Tests. It is also possible to
generate a profile of the test, from which it can be seen which functions
where not tested.
"""

import os, sys, time, pkg_resources, importlib

from ngamsLib.ngamsCore import getHostName, getNgamsVersion, \
    ngamsCopyrightString, toiso8601
from ngamsLib import ngamsConfig, ngamsHighLevelLib, ngamsLib, ngamsCore




NGAMS_TEST_MAX_TS_TIME = 900 # 15 [min]


def getTestList():
    """
    Generate a list containing the names of the test modules, which should
    be executed.

    Returns:   List with names of test Python modules (list/string).
    """
    testModList = []
    modulesInNgamsTest = pkg_resources.resource_listdir(__name__, ".")
    fileList = [f for f in modulesInNgamsTest if f.endswith("Test.py")]
    fileList.sort()
    supprTests = []
    for file in fileList:
        testMod = os.path.basename(file).split(".")[0]
        try:
            supprTests.index(testMod)
            print "===> NOTE: Test Suite: %-32s disabled - RE-ENABLE!" %\
                  testMod
            continue
        except:
            pass
        if (file.find("ngamsTest.py") == -1): testModList.append(testMod)
    return testModList


def runAllTests(notifyemail = None,
                skip = None):
    """
    Run all tests in ngams/ngamsTest.

    notifyemail:  List of email recipients that should be notified about the
                 test results (list/email addresses).

    skip:        Test Suites or Test Cases to skip. Comma separated list
                 (string).

    Returns:     Void.
    """
    skipDic = {}
    if (skip):
        for test in skip.split(","): skipDic[test] = 1
    testModList = getTestList()
    startTime = time.time()
    failModDic = {}
    noOfTests = len(testModList)
    testCount = 0
    format = "Running Test Suite: %-32s %-8s"
    line = "\nNG/AMS FUNCTIONAL TESTS - TEST REPORT\n"
    print line
    testRep = line + "\n"
    line = "Date:             %s" % toiso8601()
    print line
    testRep += line + "\n"
    line = "Host:             %s" % getHostName()
    print line
    testRep += line + "\n"
    line = "NG/AMS Version:   %s" % getNgamsVersion()
    print line
    testRep += line + "\n"
    for mod in testModList:
        if (skipDic.has_key(mod)): continue
        testCount += 1
        line = format % (mod, ("(#%d/%d)" % (testCount, noOfTests)))
        testRep += line
        sys.stdout.write(line)
        sys.stdout.flush()
        suiteStartTime = time.time()
        tstCmdLine = (sys.executable, "%s.py" % (mod,))
        try:
            stat, stdout, stderr = ngamsCore.execCmd(tstCmdLine, timeOut=NGAMS_TEST_MAX_TS_TIME, shell=False)
            testTime = (time.time() - suiteStartTime)
            if stat != 0:
                failModDic[mod] = stdout + " --- " + stderr
                stat = " - %-5.1fs - FAILURE!!\n" % testTime
            else:
                stat = " - %-5.1fs - SUCCESS\n" % testTime
        except Exception, e:
            print e
            testTime = (time.time() - suiteStartTime)
            failModDic[mod] = "TIME-OUT"
            stat = " - %-5.1fs - TIME-OUT!!\n" % testTime
        sys.stdout.write(stat)
        sys.stdout.flush()
        testRep += stat
    execTime = (time.time() - startTime)
    line = "\n\nExecuted %d Test Suites in %.3f seconds\n\n" %\
           (len(testModList), execTime)
    print line
    testRep += line

    if (failModDic):
        line = 80 * "="
        print line
        testRep += line
        line = "\n\nFAILED TEST SUITES:\n\n"
        print line
        testRep += line
        failMods = failModDic.keys()
        failMods.sort()
        for failMod in failMods:
            line = "\n%s:\n%s\n" % (failMod, failModDic[failMod])
            print line
            testRep += line + "\n"
            line = 80 * "-"
            print line
            testRep += line
        line = 80 * "="
        print line
        testRep += line

    # Send out email with test report if requested.
    if (notifyemail):
        notifyemail = notifyemail.split(",")
        par = "NgamsCfg.Server[1].RootDirectory"
        ngamsCfgObj = ngamsConfig.ngamsConfig().storeVal(par, "/NGAS")
        ngamsHighLevelLib.sendEmail(ngamsCfgObj, "localhost",
                                    "NG/AMS FUNCTIONAL TESTS REPORT",
                                    notifyemail,
                                    "ngas@%s" % ngamsLib.getCompleteHostName(),
                                    testRep)

    return failModDic


# The methods and functions listed in the exempt list can either not be tested
# or it cannot be registered that they have been executed for instance (seems
# to be the case sometimes when they're running in a thread).
def methodInExemptList(srcFile,
                       method):
    """
    Returns 1 if the method/function contained in the given source file
    is contained in the exempt list.

    srcFile:      Name of Python source file (string).

    method:       Name of method/function (string).

    Returns:      1 if the method/function is contained in the exempt list
                  otherwise 0 (integer/0|1).
    """
    exemptDic = {"ngamsSrvThreads.py":
                 ["dataCheckThread", "janitorThread", "stopDataCheckThr_"],
                 "ngamsDb.py":
                 ["setFileStatus()"]}
    if (exemptDic.has_key(srcFile)):
        try:
            exemptDic[srcFile].index(method)
            return 1
        except:
            return 0
    return 0


def correctUsage():
    """
    Print out correct usage of test program.

     Returns:   Void.
    """
    print "Input parameters for NG/AMS test program:\n"
    print "> ngamsTest [-status] " +\
           "[-tests \"<mod>,...\"] [-skip \"<mod>[.<test case>],...\" " +\
           "-notifyEmail \"<Email List>\"\n"
    print ngamsCopyrightString()


def parseCommandLine(argv):
    """
    """
    skip = None
    tests = []
    notifyemail = None
    idx = 1
    while idx < len(sys.argv):
        par = sys.argv[idx].upper()
        if (par == "-SKIP"):
            idx += 1
            skip = sys.argv[idx]
        elif (par == "-TESTS"):
            idx += 1
            tests = sys.argv[idx].split(",")
        elif (par == "-NOTIFYEMAIL"):
            idx += 1
            notifyemail = sys.argv[idx]
        else:
            correctUsage()
            sys.exit(1)
        idx += 1

    if (notifyemail == ""): notifyemail = None

    return skip, tests, notifyemail

def main():
    skip, tests, notifyemail = parseCommandLine(sys.argv)
    if tests:
        for testModName in tests:
            testMod   = importlib.import_module(testModName)
            runMethod = getattr(testMod, 'run')
            runMethod()
    else:
        failures = runAllTests(notifyemail, skip)
        sys.exit(len(failures))

if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    main()

# EOF
