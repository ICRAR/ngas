AUTOMAKE_OPTIONS = 1.8 foreign
DISTCLEANFILES = *~

INCLUDES = -I$(top_srcdir)/ngamsCClient

LDADD = $(top_srcdir)/ngamsCClient/libngams.la -lpthread

check_PROGRAMS = ngamsCClientTestArchFromMem

ngamsCClientTestArchFromMem_SOURCES = ngamsCClientTestArchFromMem.c 

#noinst_HEADERS = 

# Add here files or directories, that will be included in the distribution
EXTRA_DIST = src ref
		


# Test to be executed with "make check"
# They should return 0 test passed, else error
TESTS = ngamsCClientTestArchFromMem

# Files to be removed with command: make maintainer-clean
MAINTAINERCLEANFILES = Makefile.in
