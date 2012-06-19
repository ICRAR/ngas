#******************************************************************************
# ESO/DFS
#
# "@(#) $Id: __init__.py,v 1.1 2006/10/24 09:11:45 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  27/09/2002  Created

"""


                    #     #  #####     #     #####
                    ##    # #     #   # #   #     #
                    # #   # #        #   #  #
                    #  #  # #  #### #     #  #####
                    #   # # #     # #######       #
                    #    ## #     # #     # #     #
                    #     #  #####  #     #  #####

                        *** NGAS CONFIGURATION ***


        Copyright (c) 2001-2004, European Southern Observatory.
                            All rights reserved.


The following list shows the configuration files in use for the various
NGAS installations.
"""

import os, glob, commands

cfgList = glob.glob(os.path.dirname(__file__) + "/*.xml")

__doc__ += "\n\nConfiguration files:\n\n"
for cfgFile in cfgList:
    __doc__ += os.path.basename(cfgFile) + "\n"

    # Create a dummy .py file containing the contents of the file.
    fo = open(cfgFile)
    cfgBuf = fo.read()
    fo.close()
    
    cfgDocFile = cfgFile.replace(".", "_") + ".py"
    commands.getstatusoutput("rm -rf " + cfgDocFile)
    fo = open(cfgDocFile, "w")
    fo.write('"""\n' + cfgBuf + '\n"""\n\n# ---oOo---\n')
    fo.close()

__doc__ += "\n\nIt is possible to view the contents of these configuration\n"+\
           "files, by clicking on their corresponding links in this page.\n"

# --- oOo ---

