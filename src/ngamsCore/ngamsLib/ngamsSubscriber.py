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
# "@(#) $Id: ngamsSubscriber.py,v 1.6 2009/11/26 11:46:57 awicenec Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  04/11/2002  Created
#
"""
Contains classes to handle the information about each Subscriber and
the complete set of Subscribers.
"""

import collections
import urlparse

def validate_url(url):
    """
    Checks if the given URL is a valid subscription URL
    """

    url = url.strip()
    if not url:
        raise ValueError("url is empty")

    parse_result = urlparse.urlparse(url)

    if not parse_result.scheme:
        raise ValueError("No scheme found in URL %s. Value interpreted as %r" % (url, parse_result,))

    if not parse_result.netloc:
        raise ValueError("No netloc found in URL %s. Value interpreted as %r" % (url, parse_result,))

    if parse_result.scheme != 'http':
        msg = "%s scheme not currently supported, only http:// scheme allowed"
        raise ValueError(msg % parse_result.scheme)

_fields = ('id host_id port priority url start_date concurrent_threads active'
           ' filter_plugin filter_plugin_pars')
ngamsSubscriber = collections.namedtuple('ngamsSubscriber', _fields)
del _fields