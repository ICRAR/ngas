def pre_unpack_hook(o,b,hook='pre_unpack_hook'):
    print "\n\n\n---------------------------------------------------------------------------------------------------\n"
    print "numarray: %s in %s target %s" %(hook,b['buildout']['directory'], o['location'])
    print "\n---------------------------------------------------------------------------------------------------\n\n\n"
def post_unpack_hook(o,b,hook='post_unpack_hook'):
    print "\n\n\n---------------------------------------------------------------------------------------------------\n"
    print "numarray: %s in %s target %s" %(hook,b['buildout']['directory'], o['location'])
    print "\n---------------------------------------------------------------------------------------------------\n\n\n"
