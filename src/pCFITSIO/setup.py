#! /usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup, Extension

setup(name="pCFITSIO",
	version="0.99.9",
	description="Python Wrapper to the CFITSIO 2.0 library",
	author="Nor Pirzkal",
	author_email="npirzkal@eso.org",
	url="http://www.stecf.org/~npirzkal/python/cfitsio/",
	py_modules=["fitsio"],
	ext_modules=[Extension("pcfitsio",["pcfitsio_wrap.c","pcfitsio.c"],
				include_dirs= 	['../../include/python2.6 ','../../include/python','../../include','../../include/numarray'],
				library_dirs= ['../../lib/python','../../lib'],
				libraries=["cfitsio"]
				)])

