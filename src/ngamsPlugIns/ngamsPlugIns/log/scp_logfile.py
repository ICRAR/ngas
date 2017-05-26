#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2017
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
"""This logfile handling plug-in sends the logfile via scp to a destination"""

import logging
import os

from ngamsLib.ngamsCore import execCmd


logger = logging.getLogger(__name__)

def run(pars, srvObj, filename):
    user = pars['user']
    host = pars['host']
    tgt_dir = pars['tgt_dir']
    tgt_filename = os.path.join(tgt_dir, os.path.basename(filename))
    tgt = '%s@%s:%s' % (user, host, tgt_filename)

    execCmd(['scp', filename, tgt], timeOut=300, shell=False)

    logger.info("Successfully scp'd logfile %s to %s", filename, tgt)