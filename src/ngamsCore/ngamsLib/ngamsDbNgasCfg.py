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
        queryFormat = ("SELECT cfg_par FROM ngas_cfg_pars WHERE "
                        "cfg_group_id={0} AND cfg_par={1}")
        res = self.query2(queryFormat, args = (groupId, parName))
        if res:
            return 1
        return 0

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
        if self.hasCfgPar(groupId, parName):
            if comment:
                sql = ("UPDATE ngas_cfg_pars SET cfg_val={0}, cfg_comment={1}"
                                "WHERE cfg_group_id={2} AND cfg_par={3}")
                vals = (str(value), comment, groupId, parName)
            else:
                sql = ("UPDATE ngas_cfg_pars SET cfg_val={0}"
                                "WHERE cfg_group_id={1} AND cfg_par={2}")
                vals = (str(value), groupId, parName)
        else:
            if not comment:
                comment = ''
            if not value:
                value = ''

            sql = ("INSERT INTO ngas_cfg_pars (cfg_group_id, "
                          "cfg_par, cfg_val, cfg_comment) "
                          "VALUES ({0}, {1}, {2}, {3})")
            vals = (groupId, parName, value, comment)

        self.query2(sql, args = vals)

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
        sql = "SELECT cfg_par_group_ids FROM ngas_cfg WHERE cfg_name={0}"
        res = self.query2(sql, args = (name,))
        if not res:
            raise Exception("Configuration Error: %s not found in the DB!" % name)
        groupIds = res[0][0].split(",")
        accuRes = []
        for groupId in groupIds:
            groupId = groupId.strip()
            if not groupId:
                continue
            sql = ("SELECT cfg_group_id, cfg_par, cfg_val, cfg_comment "
                    "FROM ngas_cfg_pars WHERE cfg_group_id={0}")
            res = self.query2(sql, args = (groupId,))
            if not res:
                raise Exception("Configuration Error: %s not found in the DB!" % groupId)
            accuRes.append(res)
        return accuRes
