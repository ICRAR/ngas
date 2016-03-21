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
# "@(#) $Id: ngamsGenTestPlan.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  12/10/2004  Created
#
"""
Small tool to generate a test plan from the contents of the Test Suite
Python modules.
"""
# TODO: Make index in the beginning of the report with which there can easily
#       be jumped to Test Suites and Test Cases. In addition, links in
#       connection with the Test Suites/Cases to jump back to the index.

import getpass
import sys
import re
import pkg_resources
import importlib
import inspect

from ngamsLib.ngamsCore import getNgamsVersion
from pccUt import PccUtTime


htmlHdr = """
<body bgcolor="#FFFFFF" text="#000000">

<table align="center" width="100%%" border="0">
  <tr>
    <td><a href="/NGAS"><img src="http://jewel1:8080/NGAS/NGAS_logo_small"
    alt="NGAS logo small" title="NGAS logo small" height="54" width="200"
    border="0" /></a></td>
    <td align="right"><h2>The Next Generation Archive System</h2></td>
  </tr>
</table>
<hr>
<h1><font color="#0000FF">%s</font></h1>
<b>Date: %s</b><br>
<b>User: %s</b><br>
<b>Version: %s<br><br>\n
"""

htmlFooter = """
<hr>
</body>
</html>
"""

testSuiteFormat="""\n
<hr>
<h2><font color="#0000FF">Test Suite: %s</font></h2>
<h4><font color="#0000FF">Description:</font></h4>
<pre>%s</pre>
"""

testCaseFormat="""\n
<h3><font color="#0000FF">Test Case: %s</font></h3>
<h4><font color="#0000FF">Description:</font></h4>
<pre>%s</pre>
"""
#"


def genTestPlan(title,
                testSuitePat = None,
                testCasePatList = []):
    """
    Generate a test plan for all Test Suites and each Test Case in these.
    For the moment the out it simply a 'rough' HTML document.

    Returns:   HTML Test Plan (string).
    """
    testModuleNames = pkg_resources.resource_listdir(__name__, ".")
    testPlanList = []
    for testModuleName in testModuleNames:
        if pkg_resources.resource_isdir(__name__, testModuleName): continue
        if (testModuleName == "ngamsTest"): continue
        if not re.match("^ngams.*Test.py$", testModuleName): continue
        if (testSuitePat):
            if (testModuleName.find(testSuitePat) != 0): continue

        testModule = importlib.import_module('ngamsTest.' + testModuleName)

        docList = []
        for method in inspect.getmembers(testModule, inspect.isfunction):
            methodName = method.__name__
            if (methodName.find("test_") == 0):
                if (testCasePatList):
                    match = 0
                    for testCasePat in testCasePatList:
                        if (methodName.find(testCasePat) == 0):
                            match = 1
                            break
                else:
                    match = 1
                if (match):
                    docList.append([methodName, method.__doc__])
        testPlanList.append([testModuleName, testModule.__doc__, docList])

    testPlanList.sort()
    testPlan = htmlHdr % (title, PccUtTime.TimeStamp().getTimeStamp(),
                          getpass.getuser(), getNgamsVersion())
    for testSuite in testPlanList:
        testPlan += testSuiteFormat % (testSuite[0], testSuite[1])
        for testCase in testSuite[2]:
            testPlan += testCaseFormat % (testCase[0], testCase[1])
    testPlan += htmlFooter
    return testPlan


def correctUsage():
    """
    Print out correct usage.

    Returns:  Void.
    """
    print "\nCorrect usage is:\n"
    print "> python ngasGenTestPlan.py [-testSuite <Test Suite> " +\
              "[-testCases \"<Test Case 1>, ...\" | <Test Case Pattern>]]"
    print ""
    print "The Test Suite/Cases can be given as patterns.\n"


def execute(argv):
    """
    Execute the tool on the given input parameters.

    argv:     The value of sys.argv (list).

    Returns:  Void.
    """
    testSuite = None
    testCases = []
    idx = 1
    title = "NG/AMS Functional Test Plan"
    while idx < len(argv):
        par = argv[idx].upper()
        if (par == "-TESTSUITE"):
            idx += 1
            testSuite = argv[idx]
        elif (par == "-TESTCASES"):
            idx += 1
            testCases = argv[idx].split(",")
        elif (par == "-TITLE"):
            idx += 1
            title = argv[idx]
        else:
            correctUsage()
            sys.exit(1)
        idx += 1
    print genTestPlan(title, testSuite, testCases)


if __name__ == '__main__':
    """
    Main program executing the test cases of the module test.
    """
    execute(sys.argv)


# EOF
