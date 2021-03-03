

def getMacAddr(interface):
    """
    Function returns the MAC address (hardware address) of an
    network interface card.

    CALLING: MAC = getMacAddr(interface)

    """


    import IN,fcntl,socket,struct,sys

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        result = fcntl.ioctl(s.fileno(), IN.SIOCGIFHWADDR, interface + \
		'\0'*256)
    except IOError,e:
    print 'IO ERROR occured: ',e
    s.close()
    return 'xx:xx:xx:xx'
    except:
    print 'Unspecified error occured: ',sys.exc_info()[1]
    s.close()
    return 'xx:xx:xx:xx'
    s.close()

    H_MACADDR = result[18:24]
    MACADDR = ''

    for c in range(len(H_MACADDR)):
    lc = hex(struct.unpack('B',H_MACADDR[c])[0])[2:].upper()
    if len(lc) == 1:
        lc = '0' + lc
    MACADDR = MACADDR + lc + ':'

    return MACADDR[0:-1]

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 2:
        print getMacAddr(sys.argv[1])
    else:
        print "getMacAddr <interface>"
