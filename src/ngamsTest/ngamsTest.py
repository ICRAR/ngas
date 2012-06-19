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

# TODO: Implement a test DB in which times for executing the tests are stored.
#       There should be a window of 100 test runs per node per test suite.
#       This information will be printed out when executing the tests in the
#       test report.


import os, sys, glob, getpass, time
import cProfile
#pstats
import pcc, PccUtUtils, PccUtTime
from ngams import *
import ngamsLib, ngamsConfig, ngamsHighLevelLib
try:
    import ngamsTestLib
except Exception, e:
    print str(e)
    sys.exit(1)


NGAMS_TEST_MAX_TS_TIME = 2000


def getTestList():
    """
    Generate a list containing the names of the test modules, which should
    be executed.

    Returns:   List with names of test Python modules (list/string).
    """
    testModList = []
    globPat = os.path.normpath(NGAMS_SRC_DIR + "/ngamsTest/*Test.py")
    fileList = glob.glob(globPat)
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


def runAllTests(notifEmail = None,
                skip = None):
    """
    Run all tests in ngams/ngamsTest.

    notifEmail:  List of email recipients that should be notified about the
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
    line = "Date:             %s" % PccUtTime.TimeStamp().getTimeStamp()
    print line
    testRep += line + "\n"
    line = "Host:             %s" % getHostName()
    print line
    testRep += line + "\n"
    line = "NG/AMS Version:   %s" % getNgamsVersion()
    print line
    testRep += line + "\n"    
    line = "DB Configuration: %s\n" % str(ngamsTestLib.getRefCfg())
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
        tstCmdLine = "python " + mod + ".py -cfg %s" % ngamsTestLib.getRefCfg()
        stat, stdout, stderr = PccUtUtils.execCmd(tstCmdLine,
                                                  NGAMS_TEST_MAX_TS_TIME)
        testTime = (time.time() - suiteStartTime)
        if (testTime >= NGAMS_TEST_MAX_TS_TIME):
            failModDic[mod] = "TIME-OUT"
            stat = " - %-5.1fs - TIME-OUT!!\n" % testTime
        elif ((stdout.find("FAILED") != -1) or (stat != 0)):
            failModDic[mod] = stdout + " --- " + stderr
            stat = " - %-5.1fs - FAILURE!!\n" % testTime
        else:
            stat = " - %-5.1fs - SUCCESS\n" % testTime
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
    if (notifEmail):
        notifEmail = notifEmail.split(",")
        par = "NgamsCfg.Server[1].RootDirectory"
        ngamsCfgObj = ngamsConfig.ngamsConfig().storeVal(par, "/NGAS")
        ngamsHighLevelLib.sendEmail(ngamsCfgObj, "localhost",
                                    "NG/AMS FUNCTIONAL TESTS REPORT",
                                    notifEmail,
                                    "ngas@%s" % ngamsLib.getCompleteHostName(),
                                    testRep)


def getAllSrcFiles():
    """
    Generate a dictionary containing a reference to all function and method
    calls of the NG/AMS SW (relevant for the Unit Testing).

    The dictionary returned has keys with the format:

        '<src file>:<line no>(fct name)'

    The entry for each function/method is a tuple with the following
    information:

        (<module name>, <src file>, <line no>, <fct name>)

    Returns:   Dictionary with information about methods and functions
               of the NG/AMS SW (dictionary/tuple).
    """
    modules = ["ngamsLib", "ngamsPClient", "ngamsServer"]
    fctDic = {}
    for mod in modules:
        modDir = os.path.normpath(NGAMS_SRC_DIR + "/" + mod + "/*.py")
        exitCode, stdOut, stdErr = PccUtUtils.execCmd("grep -n def " + modDir)
        fcts = stdOut.split("\n")
        for fct in fcts:
            fct = trim(fct, " :")
            if ((fct != "") and (fct.find("def ") != -1)):
                fctEls  = fct.split(":")
                complSrcFile = fctEls[0].split("/")
                modName = complSrcFile[-2]
                srcFile = complSrcFile[-1]
                lineNo  = fctEls[1]
                fctName = fctEls[-1].split(" ")[-1].split("(")[0]
                if (fctName != ""):
                    key = srcFile + ":" + str(lineNo) + "(" + fctName + ")"
                    fctDic[key] = (modName, srcFile, lineNo, fctName)
    return fctDic
    

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

 
def genStatus():
    """
    Execute all the tests and generate a test status, indicating which
    methods/function are still not tested.

    Returns:    Test report (string).
    """
    # List of method/functions, which for some reason should not
    # appear in the status, normally because they have been executed
    # but could not be registered (for instance if executed within a thread).
    repFileDic = {}
    testModList = getTestList()
    for mod in testModList:
        repFileDic[mod] = "/tmp/" + mod + "_" + getpass.getuser() + ".status"
        os.system("rm -rf " + repFileDic[mod])

        # Execute the test
        profile.run("import " + mod + "\n" + mod + ".run()", repFileDic[mod])

    # Generate one report from the reports in /tmp
    repList = ""
    statObj = None
    for mod in repFileDic.keys():
        if (statObj == None):
            statObj = pstats.Stats(repFileDic[mod])
        else:
            statObj.add(repFileDic[mod])
    for mod in repFileDic.keys():
        ngamsTestLib.remove(repFileDic[mod])
    statObj.strip_dirs() 
    #statObj.print_stats()
    proWidth, proList = statObj.get_print_list(())
    testedFctDic = {}
    for fctEntry in proList:
        if (fctEntry[0].find("ngams") != -1):
            key = fctEntry[0] + ":" + str(fctEntry[1]) + "(" + fctEntry[2] +")"
            testedFctDic[key] = (fctEntry[0], fctEntry[1], fctEntry[2])

    # Figure out which functions/methods were exercised by the test and
    # which were not.
    ngamsFctDic = getAllSrcFiles()
    tmpDic = {}
    for fctRef in ngamsFctDic.keys():
        if (not testedFctDic.has_key(fctRef)):
            tmpDic[ngamsFctDic[fctRef][1] + ngamsFctDic[fctRef][3]] =\
                                          ngamsFctDic[fctRef]
    fileList = tmpDic.keys()
    fileList.sort()

    # Give out status of functions/methods that apparently were not
    # executed during the tests.
    report = "METHODS AND FUNCTIONS NOT EXECUTED DURING TEST:\n\n"
    testedFcts = len(testedFctDic.keys())
    totalFcts  = len(ngamsFctDic.keys())
    #report += "Test Coverage for Method/Function Invocation: " +\
    #          str(100 * testedFcts / totalFcts) + "% " +\
    #          "(" + str(testedFcts) + "/" + str(totalFcts) + ")\n\n"
    report += "\n"
    format = "%-25s %-12s %-25s\n"
    sep1 = 70 * "-"
    report += format % ("Source File:", "Line No:", "Method/Function:")
    sepCtrlDic = {}
    for key in fileList:
        subRep = ""
        srcFile = tmpDic[key][1]
        lineNo = str(tmpDic[key][2])
        method = tmpDic[key][3] + "()"
        if (not methodInExemptList(srcFile, method[:-2])):
            subRep += format % (srcFile, lineNo, method)
        if ((subRep != "") and (not sepCtrlDic.has_key(tmpDic[key][1]))):
            report += sep1 + "\n"
            sepCtrlDic[tmpDic[key][1]] = 1
        report += subRep
    print "\n\n" + report + "\n"
    

def correctUsage():
     """
     Print out correct usage of test program.

     Returns:   Void.
     """
     print "Input parameters for NG/AMS test program:\n"
     print "> ngamsTest [-status] " +\
           "[-tests \"<mod>,...\"] [-skip \"<mod>[.<test case>],...\" " +\
           "-notifEmail \"<Email List>\" -cfg <Ref. Cfg. File>\n"
     print ngamsCopyrightString()
    

def parseCommandLine(argv):
    """
    """
    skip = None
    status = 0
    tests = []
    notifEmail = None
    cfg = None
    idx = 1
    while idx < len(sys.argv):
        par = sys.argv[idx].upper()
        if (par == "-SKIP"):
            idx += 1
            skip = sys.argv[idx]
        elif (par == "-STATUS"):
            status = 1
        elif (par == "-TESTS"):
            idx += 1
            tests = sys.argv[idx].split(",")
        elif (par == "-NOTIFEMAIL"):
            idx += 1
            notifEmail = sys.argv[idx]
        elif (par == "-CFG"):
            idx += 1
            cfg = sys.argv[idx]
        else:
            correctUsage()
            sys.exit(1)
        idx += 1
    if (not notifEmail):
        raise Exception, "Specify parameter: -notifEmail <Rec List>"
    if (not cfg):
        raise Exception, "Specify parameter: -cfg <Ref. Cfg. File>"

    if (notifEmail == ""): notifEmail = None

    return skip, status, tests, notifEmail, cfg

def main():
    skip, status, tests, notifEmail, cfg = parseCommandLine(sys.argv)
    ngamsTestLib.setRefCfg(cfg)
    if (status):
        genStatus()
    elif (tests != []):
        for testMod in tests:
            exec "import " + testMod
            exec testMod + ".run()"
    else:
        runAllTests(notifEmail, skip)

if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    main()

# EOF
