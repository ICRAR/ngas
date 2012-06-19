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
# "@(#) $Id: ngamsTestPstats.py,v 1.3 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  23/04/2002  Created
#

"""
This module contains a utility class for doing profiling on the NG/AMS
Unit Tests.
"""

import pstats


class ngamsTestPstats(pstats.Stats):
    """
    NG/AMS inherited profiling report generator.
    """
    
    def __init__(self,
                 *args):
        """
        Constructor method.

        args:   Tuple with filenames used to build the report (tuple)
        """
        pstats.Stats.__init__(self, args)

       
    def get_print_list(self,
                       sel_list):
        """
        """
        width = self.max_name_len
        if self.fcn_list:
            list = self.fcn_list[:]
            msg = "   Ordered by: " + self.sort_type + '\n'
        else:
            list = self.stats.keys()
            msg = "   Random listing order was used\n"

        for selection in sel_list:
            list,msg = self.eval_print_amount(selection, list, msg)

        count = len(list)

        if not list:
            return 0, list
        if count < len(self.stats):
            width = 0
            for func in list:
                if  len(func_std_string(func)) > width:
                    width = len(func_std_string(func))
        return width+2, list
        

# EOF
