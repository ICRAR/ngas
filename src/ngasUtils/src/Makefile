#
#    ALMA - Atacama Large Millimiter Array
#    (c) European Southern Observatory, 2002
#    Copyright by ESO (in the framework of the ALMA collaboration),
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
# "@(#) $Id: Makefile,v 1.3 2008/12/15 22:03:38 jknudstr Exp $"
#
# Who       When        What
# --------  ----------  -------------------------------------------------------
# jknudstr  08/08/2005  Created

# Include directories
INC_DIRS = 

# Library directories
LIB_DIRS = 

# Install directories
ifneq ($(INSTROOT),)
	PREFIX=$(INSTROOT)
else
	#PREFIX=/opsw/packages
	echo "please specify prefix directory"
endif
ifneq ($(INTROOT),)
	PREFIX=$(INTROOT)
endif
BINDIR=$(PREFIX)/bin
# - where to put man pages
MANDIR=$(PREFIX)/man
# - where to put lib
LIBDIR=$(PREFIX)/lib
# - where to put include
INCDIR=$(PREFIX)/include

# Compiler
CC = gcc

# Compile flags
CDEBUGFLAGS = -ansi -Wall -fPIC -g -DDEBUG -O -ansi 

# Defines
DEFINES= -D_XOPEN_SOURCE -D_XOPEN_SOURCE_EXTENDED -D_REENTRANT

# Solaris
ifeq ($(shell uname),SunOS)
	SYSLIBS= /usr/lib/libsocket.a -lnsl
endif

# Mostly standard
CP = cp -f
CHMOD= chmod
MKDIR= mkdir -p
AR = ar
RANLIB = ranlib
TAR= gtar


CFLAGS = $(CDEBUGFLAGS) $(INC_DIRS) $(LIB_DIRS) $(DEFINES)
LDFLAGS= $(CFLAGS) -L.

LIBOBJS = 

TARGETS = 

all:    $(TARGETS)


install: $(TARGETS)
	$(MKDIR) $(BINDIR)
	$(CP) ngasArchiveClient.py $(BINDIR)/ngasArchiveClient
	$(CHMOD) a+x $(BINDIR)/ngasArchiveClient
	$(CP) ngasArchiveFileList.py $(BINDIR)/ngasArchiveFileList
	$(CHMOD) a+x $(BINDIR)/ngasArchiveFileList
	$(CP) ngasArchiveTool.py $(BINDIR)/ngasArchiveTool
	$(CHMOD) a+x $(BINDIR)/ngasArchiveTool
	$(CP) ngasChangeDiskLabel.py $(BINDIR)/ngasChangeDiskLabel
	$(CHMOD) a+x $(BINDIR)/ngasChangeDiskLabel
	$(CP) ngasCheckFileCopies.py $(BINDIR)/ngasCheckFileCopies
	$(CHMOD) a+x $(BINDIR)/ngasCheckFileCopies
	$(CP) ngasCheckFileList.py $(BINDIR)/ngasCheckFileList
	$(CHMOD) a+x $(BINDIR)/ngasCheckFileList
	$(CP) ngasCloneFileList.py $(BINDIR)/ngasCloneFileList
	$(CHMOD) a+x $(BINDIR)/ngasCloneFileList
	$(CP) ngasDiscardFiles.py $(BINDIR)/ngasDiscardFiles
	$(CHMOD) a+x $(BINDIR)/ngasDiscardFiles
	$(CP) ngasDiskFormat.py $(BINDIR)/ngasDiskFormat
	$(CHMOD) a+x $(BINDIR)/ngasDiskFormat
	$(CP) ngasPrepareVolume.py $(BINDIR)/ngasPrepareVolume
	$(CHMOD) a+x $(BINDIR)/ngasPrepareVolume
	$(CP) ngasRegisterExtFiles.py $(BINDIR)/ngasRegisterExtFiles
	$(CHMOD) a+x $(BINDIR)/ngasRegisterExtFiles
	$(CP) ngasRetireDisk.py $(BINDIR)/ngasRetireDisk
	$(CHMOD) a+x $(BINDIR)/ngasRetireDisk
	$(CP) ngasSetCompleted.py $(BINDIR)/ngasSetCompleted
	$(CHMOD) a+x $(BINDIR)/ngasSetCompleted
	$(CP) ngasVerifyCloning.py $(BINDIR)/ngasVerifyCloning
	$(CHMOD) a+x $(BINDIR)/ngasVerifyCloning

clean: 
	rm -f $(TARGETS)
	rm -f *.tgz
	rm -f *.pyc
	rm -f *.o
	rm -f *~
	rm -f #*
	rm -f core*

# EOF
