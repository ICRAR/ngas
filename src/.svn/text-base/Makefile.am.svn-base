## Process this file with automake to produce Makefile.in

##   Copyright (C) 2001-2004 European Southern Observatory

##   This program is free software; you can redistribute it and/or modify
##   it under the terms of the GNU General Public License as published by
##   the Free Software Foundation; either version 2 of the License, or
##   (at your option) any later version.

##   This program is distributed in the hope that it will be useful,
##   but WITHOUT ANY WARRANTY; without even the implied warranty of
##   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##   GNU General Public License for more details.

##   You should have received a copy of the GNU General Public License
##   along with this program; if not, write to the Free Software
##   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



# AUTOMAKE_OPTIONS= 1.8 minimum expected version, 
#                  foreign is most relaxed strictness
AUTOMAKE_OPTIONS = 1.8 foreign 


# Include local macros implemented in your directory, i.e. m4macros
# ACLOCAL_AMFLAGS = -I m4macros

# DISTCLEANFILES=  remove also *~ when execute make clean
DISTCLEANFILES = *~            

# Build on subdirectories:
# libsrc: library sources
# binsrc: executable sources
# incl: exportable includes
# doc: documentaton
# scripts: directory with script files
# test: unit test directory
SUBDIRS= ngamsCClient ngamsJClient/src ngamsTest

# Include the test for qfits options in the distribution
EXTRA_DIST = VERSION COPYRIGHT BUGS LICENSE

# Files to be removed with command: make maintainer-clean
MAINTAINERCLEANFILES = Makefile.in aclocal.m4 config.h.in config.log \
        config.status configure stamp-h.in configure.scan autoscan.log \
        admin/config.guess admin/config.sub admin/depcomp admin/install-sh \
        admin/ltmain.sh admin/missing

# EOF
