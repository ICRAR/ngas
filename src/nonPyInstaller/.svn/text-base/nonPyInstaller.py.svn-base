# -*- coding: utf-8 -*-
import os
CRT_DIR = os.path.normpath(os.path.dirname(os.path.abspath(__file__)))
NGAS_DIR = os.path.normpath(CRT_DIR + "/../..")
CFG_CMD = "./configure --prefix=" + NGAS_DIR

def install_sqlite():
    #install sqlite
    os.chdir(NGAS_DIR + "/clib_tars")
    print "changed into clib_tars directory."
    os.system("tar -xvf sqlite-autoconf-3070400.tar.gz")
    print "sqlite-autoconf-3070400.tar.gz extracted."
    os.chdir("./sqlite-autoconf-3070400")
    print "changed into sqlite-autoconf-3070400 directory."
    os.system(CFG_CMD)
    print "configuration completed."
    os.system("make")
    print "make completed."
    os.system("make install")
    print "make install completed."
    print "\n\n\n------------------------------\nsqlite installed\n------------------------------\n\n\n"

def install_cfitsio():
    #install cfitsio
    os.chdir(NGAS_DIR + "/clib_tars")
    print "changed into clib_tars directory."
    os.system("tar -xvf cfitsio3250.tar.gz")
    print "cfitsio3250.tar.gz extracted."
    os.chdir("./cfitsio")
    print "changed into cfitsio3250 directory."
    os.system(CFG_CMD)
    print "configuration completed."
    os.system("make")
    print "make completed."
    os.system("make install")
    print "make install completed."
    print "\n\n\n------------------------------\ncfitsio installed\n------------------------------\n\n\n"

def install_chksum():
    #install chksum
    os.chdir(NGAS_DIR + "/clib_tars")
    print "changed into clib_tars directory."
    os.system("tar -xvf chksum.tar.gz")
    print "chksum.tar.gz extracted."
    os.chdir("./chksum/src")
    print "changed into chksum/src directory."
    os.putenv("INSTROOT",NGAS_DIR)
    print "Environment Variable set."
    os.system("make all")
    print "make completed."
    os.system("make install")
    print "make install completed."
    print "\n\n\n------------------------------\nchksum installed\n------------------------------\n\n\n"

def install_CJClients():
    #install CClient, JClient and ngasTest
    os.chdir(NGAS_DIR + "/src")
    print "changed into src directory."
    os.system("./bootstrap")
    print "bootstrap completed."
    CFG_CMD_CJ = "./configure --prefix=" + NGAS_DIR
    os.system(CFG_CMD_CJ)
    print "configuration completed."
    os.system("make all")
    print "make completed."
    os.system("make install")
    print "make install completed."
    #BUILD_LINK = "ln -sf " + NGAS_DIR + "/src/ngamsCClient/ngamsCClient " + NGAS_DIR + "/bin/NGAS_CClient"
    #os.system(BUILD_LINK)
    #print "Links in bin built."
    print "\n\n\n------------------------------\nC&J Clients installed\n------------------------------\n\n\n"
    
def install_pcfitsio():
    #install numarray first
    os.chdir(NGAS_DIR + "/clib_tars")
    print "changed into clib_tars directory."
    os.system("tar -xvf numarray-1.5.2.tar.gz")
    print "numarray-1.5.2.tar.gz extracted."
    os.chdir("./numarray-1.5.2")
    print "changed into numarray-1.5.2 directory."
    INSTALL_NUMARRAY_CMD = "python setup.py config install --gencode --home="+NGAS_DIR
    os.system(INSTALL_NUMARRAY_CMD)
    print "numarray installation completed."
    
    #install pcfitsio
    os.chdir(NGAS_DIR + "/src/pCFITSIO")
    print "changed into src/pCFITSIO directory."
    INSTALL_PFITS_CMD = "python setup.py install --home=" + NGAS_DIR
    os.system(INSTALL_PFITS_CMD)
    print "pcfitsio installation completed."

def main():
    #install all parts one by one
    install_sqlite()
    install_cfitsio()
    install_chksum()
    install_CJClients()
    install_pcfitsio()

if __name__ == '__main__':
    main()
