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
# "@(#) $Id: __init__.py,v 1.2 2008/08/19 20:37:45 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  27/09/2002  Created


_doc =\
"""


                    #     #  #####     #     #####
                    ##    # #     #   # #   #     #
                    # #   # #        #   #  #
                    #  #  # #  #### #     #  #####
                    #   # # #     # #######       #
                    #    ## #     # #     # #     #
                    #     #  #####  #     #  #####

                        *** NGAS UTILTIIES ***


%s


Python Init file for the ngasUtils module making it possible to load
sub-modules within this module. In order to do this, the following import
statements must be invoked:

from ngasUtils import *
import <Specific Python Module>


Most of the tools in the NGAS Utilities Modules, use a resource file.
This must be stored in the home directory of the user running the tool,
under the name: '$HOME/.ngas'.

The format of the NGAS Utilities Resource file is:

<Parameter>=<Value>

The parameters defined are:

===============================================================
Parameter          Description
---------------------------------------------------------------
AccessCode         Encrypted Access Code(*).

DbServer           Name of DB server.

DbUser             Name of DB user.

DbPassword         Encrypted password(*).

DbName             Name of DB on the given DB server to use.

SmtpHost           Email server address.

EmailNotification  List of recipients of Email Notification
                   Messages produced by the tools.

NgasHost           Name of NGAS Host which the NGAS Utilities
                   should contact.

NgasPort           Port number used by the NG/AMS Server on
                   the specified NGAS Host.


*: For the issue of encrypting passwords for the NGAS System, contact
   ngast@eso.org.
"""
try:
    from ngamsLib.ngamsCore import ngamsCopyrightString
    __doc__ = _doc % ngamsCopyrightString()
except:
    copRight =\
             "           ALMA - Atacama Large Millimiter Array" +\
             "          (c) European Southern Observatory, 2002"+\
             " Copyright by ESO (in the framework of the ALMA " +\
             "collaboration),"+\
             "                    All rights reserved"
    __doc__ = _doc % copRight



# EOF
