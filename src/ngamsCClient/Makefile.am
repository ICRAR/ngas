AUTOMAKE_OPTIONS = 1.8 foreign
DISTCLEANFILES = *~

# The initial library
lib_LTLIBRARIES = libngams.la
libngams_la_LDFLAGS = -version-info $(LT_CURRENT):$(LT_REVISION):$(LT_AGE)
libngams_la_SOURCES = \
		ngamsCClientLib.c \
		ngamsLicense.txt.c

# Individual programs and their dependencies
bin_PROGRAMS =  ngamsCClient ngamsCClientX ngamsArchiveClient ngamsCrc32

ngamsCClient_SOURCES = ngamsCClient.c ngamsCClientMan.txt.c
ngamsCClient_LDADD = libngams.la

ngamsCClientX_SOURCES = ngamsCClientX.c ngamsCClientXMan.txt.c
ngamsCClientX_LDADD = libngams.la

ngamsArchiveClient_SOURCES = ngamsArchiveClient.c ngamsArchiveClientMan.txt.c
ngamsArchiveClient_LDADD = libngams.la

ngamsCrc32_SOURCES = ngamsCrc32.c
ngamsCrc32_LDFLAGS = -lz

# Files to be installed under <prefix>/include
include_HEADERS = ngams.h

# Files not to be installed under <prefix>/include, but to be added to the
# distribution
noinst_HEADERS = ngamsArchiveClient.h

EXTRA_DIST = ngamsLicense.doc ngamsArchiveClient.doc ngamsCClientX.doc ngamsCClient.doc

ngamsCClientLib.c: ngamsVERSION.h

%.txt.c: %.doc
	@-(rm -f $@)
	@echo "Generating $@"
	@-(echo  "static const char *txt =" >> $@)
	@-(sed 's/"/\\"/g; s/.*/    "&\\n"/' $< >> $@)
	@-(echo  ";" >> $@)
	@-(echo  "const char *$*(void)" >> $@)
	@-(echo  "{" >> $@)
	@-(echo  "  return txt;" >> $@)
	@-(echo  "}" >> $@)

ngamsVERSION.h:
	@-(rm -f ngamsVERSION.h)
	@-(cat ../../VERSION > ngamsVERSION.h)

CLEANFILES= ngamsVERSION.h *.txt.c

# Files to be removed with command: make maintainer-clean
MAINTAINERCLEANFILES = Makefile.in aclocal.m4 config.h.in config.log \
        config.status configure stamp-h.in configure.scan autoscan.log \
        admin/config.guess admin/config.sub admin/depcomp admin/install-sh \
        admin/ltmain.sh admin/missing

# EOF
