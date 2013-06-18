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

import logging,os

logger = logging.getLogger('ngasdaemonlogger')
try:
    # try the standard location first (requires root)
    hdlr = logging.FileHandler('/var/log/ngasdaemonlogger.log')
except IOError as e:
    try:
        # fallback to home directory
        os.makedirs('%s/var/log' % os.environ['HOME'])
    except OSError:
        pass
    hdlr = logging.FileHandler('%s/var/log/ngasdaemonlogger.log' % 
                               os.environ['HOME'])
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

class ngaslog:
    """
    Just a simple logging class for the startup daemon.
    """        
    def __init__(self, logtype, message):
            self.logtype = logtype
            self.logmessage = message
            if self.logtype == 'ERROR':
                logger.error(self.logmessage)
            else:
                logger.info(self.logmessage)

