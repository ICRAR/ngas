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
# "@(#) $Id: ngamsDbNgasCfg.py,v 1.4 2008/08/19 20:51:50 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  07/03/2008  Created
#

"""
Contains queries for accessing the NGAS Configuration Tables.

This class is not supposed to be used standalone in the present implementation.
It should be used as part of the ngamsDbBase parent classes.
"""

from   ngams import *
import ngamsDbCore


class ngamsDbNgasCfg(ngamsDbCore.ngamsDbCore):
    """
    Contains queries for accessing the NGAS Configuration Tables.
    """
  
    def hasCfgPar(self,
                  groupId,
                  parName):
        """
        Return 1 if the given configuration parameter (given by its
        Simplified XPath name) and Configuration Group ID is defined in the
        configuration table in the DB.

        groupId:      Group ID for the parameter (string).
        
        parName:      Name of parameter (string).

        Returns:      1 = parameter defined, 0 = parameter not defined
                      (integer/0|1).
        """
        queryFormat = "SELECT cfg_par FROM ngas_cfg_pars WHERE " +\
                   "cfg_group_id='%s' AND cfg_par='%s'"
        sqlQuery = queryFormat % (groupId, parName)
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            return 0
        else:
            return 1

    
    def writeCfgPar(self,
                    groupId,
                    parName,
                    value,
                    comment):
        """
        Write a configuration parameter to the NGAS DB. If the parameter
        is already defined, the value/comment are updated.

        groupId:   Configuration Group ID (string).
        
        parName:   Name of parameter (string).
        
        value:     Value of parameter (string).
        
        comment:   Comment for parameter (string).
        
        Returns:   Reference to object itself.
        """
        if (self.hasCfgPar(groupId, parName)):
            queryFormat = "UPDATE ngas_cfg_pars SET cfg_val='%s'"
            if (comment): queryFormat += ", cfg_comment='%s'"
            queryFormat += " WHERE cfg_group_id='%s' AND cfg_par='%s'"
            if (comment):
                sqlQuery = queryFormat % (str(value), comment, groupId,
                                          parName)
            else:
                sqlQuery = queryFormat % (str(value), groupId, parName)
        else:
            if (not comment): comment = ""
            if (not value): value = ""
            queryFormat = "INSERT INTO ngas_cfg_pars (cfg_group_id, " +\
                          "cfg_par, cfg_val, cfg_comment) " +\
                          "VALUES ('%s', '%s', '%s', '%s')"
            sqlQuery = queryFormat % (groupId, parName, value, comment)
        self.query(sqlQuery)
        return self


    def getCfgPars(self,
                   name):
        """
        Return the list of configuration parameters from the DB associated
        to the given name.

        name:     Name of the configuration association (string).

        Returns:  List with sub-lists with the information. The format is:

                    [[<Group ID>, <Parameter>, <Value>, <Comment>], ...]

                    (list).
        """
        # We should query the parameters according to the order in which
        # the Cfg. Group IDs are listed.
        sqlQuery = "SELECT cfg_par_group_ids FROM ngas_cfg WHERE " +\
                   "cfg_name='" + name + "'"
        res = self.query(sqlQuery, ignoreEmptyRes=0)
        if (res == [[]]):
            raise Exception, "Referenced configuration: " + name + " not " +\
                  "found in the DB!"
        groupIds = res[0][0][0].split(",")
        accuRes = []
        for groupId in groupIds:
            groupId = groupId.strip()
            if (not groupId): continue
            sqlQuery = "SELECT cfg_group_id, cfg_par, cfg_val, cfg_comment " +\
                       "FROM ngas_cfg_pars WHERE cfg_group_id='" + groupId +"'"
            res = self.query(sqlQuery, ignoreEmptyRes=0)
            accuRes += res[0]
        return accuRes


# EOF
