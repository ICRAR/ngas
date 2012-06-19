def pre_unpack_hook(o,b,hook='pre_unpack_hook'):
    print "\n\n\n---------------------------------------------------------------------------------------------------\n"
    print "CJClient: %s in %s target %s" %(hook,b['buildout']['directory'], o['location'])
    print "\n---------------------------------------------------------------------------------------------------\n\n\n"
def post_unpack_hook(o,b,hook='post_unpack_hook'):
    print "\n\n\n---------------------------------------------------------------------------------------------------\n"
    print "CJClient: %s in %s target %s" %(hook,b['buildout']['directory'], o['location'])
    print "\n---------------------------------------------------------------------------------------------------\n\n\n"
def pre_configure_hook(o,b,hook='pre_configure_hook'):
    print "\n\n\n---------------------------------------------------------------------------------------------------\n"
    print "CJClient: %s in %s target %s" %(hook,b['buildout']['directory'], o['location'])
    print "\n---------------------------------------------------------------------------------------------------\n\n\n"
def pre_make_hook(o,b,hook='pre_make_hook'):
    print "\n\n\n---------------------------------------------------------------------------------------------------\n"
    print "CJClient: %s in %s target %s" %(hook,b['buildout']['directory'], o['location'])
    print "\n---------------------------------------------------------------------------------------------------\n\n\n"
def post_build_hook(o,b,hook='post_build_hook'):
    print "\n\n\n---------------------------------------------------------------------------------------------------\n"
    print "CJClient: %s in %s target %s" %(hook,b['buildout']['directory'], o['location'])
    print "\n---------------------------------------------------------------------------------------------------\n\n\n"
def post_make_hook(o,b,hook='post_make_hook'):
    print "\n\n\n---------------------------------------------------------------------------------------------------\n"
    print "CJClient: %s in %s target %s" %(hook,b['buildout']['directory'], o['location'])
    print "\n---------------------------------------------------------------------------------------------------\n\n\n"
