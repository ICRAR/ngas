#!/opsw/util/bin/python
"""
       Program to construct and broadcast the Wake-On-LAN package over UDP for a
       specified MAC address.

       Synposis: wolpython [-m <mac_address>] [-h]

       Options:
         -m --mac:   string in standard TCP hex notation.
         -h --help:  flag, if set this help text is displayed.

       Example:
         wolpython -m '00:03:93:81:68:b2'
"""
# Wake-On-LAN
#
# Copyright (C) 2002 by Micro Systems Marc Balmer
# Written by Marc Balmer, marc@msys.ch, http://www.msys.ch/
# This code is free software under the GPL
# Modifications by Andreas Wicenec [ESO]

import struct, socket

def WakeOnLan(ethernet_address):

  # Construct a six-byte hardware address

  addr_byte = ethernet_address.split(':')
  hw_addr = struct.pack('BBBBBB', int(addr_byte[0], 16),
    int(addr_byte[1], 16),
    int(addr_byte[2], 16),
    int(addr_byte[3], 16),
    int(addr_byte[4], 16),
    int(addr_byte[5], 16))

  # Build the Wake-On-LAN "Magic Packet"...

  msg = '\xff' * 6 + hw_addr * 16

  # ...and send it to the broadcast address using UDP

  s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
  s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
  s.sendto(msg, ('<broadcast>', 9))
  s.close()

def usage():
  	"""
	The usage 'trick' function just calls pydoc to get the in-line documentation.
	"""
	import pydoc
	print pydoc.help('wolpython')
	sys.exit()



if __name__ == "__main__":
# Example use
#  WakeOnLan('0:3:93:81:68:b2')
        import getopt,sys
	if len(sys.argv) == 1: usage()
        opts,args = getopt.getopt(sys.argv[1:],"m:h",\
                   ["mac","help"])

        for o,v in opts:
                if o in ("-m","--mac"):
                        _MAC_ = v
                elif o in ("-h","--help"):
			usage()
		else:
			usage()

	WakeOnLan(_MAC_)

