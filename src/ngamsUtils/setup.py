#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
#    Copyright by UWA (in the framework of the ICRAR)
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
import os
import sys

from setuptools import setup, find_packages

with open('../../VERSION') as vfile:
    for line in vfile.readlines():
        if "ngamsNGAMS_SW_VER" in line:
            version = line.split("NGAMS_SW_VER ")[1].strip()[1:-1]
            break

setup(
    name='ngamsUtils',
    version=version,
    description="The python package containing the NGAS utility python scripts",
    long_description="The python package containing the NGAS utility python scripts",
    classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='',
    author_email='',
    url='',
    license='LGPL',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'ngamsUtils' : ['README']
    },
    install_requires=['ngamsCore', 'ngamsPClient'],
    entry_points= {
        'console_scripts':[
            'ngas-check-file-list=ngamsUtils.ngasCheckFileList:main',
            'ngas-check-file-copies=ngamsUtils.ngasCheckFileCopies:main',
            'ngas-discard-files=ngamsUtils.ngasDiscardFiles:main',
            'ngas-verify-cloning=ngamsUtils.ngasVerifyCloning:main',
            'ngas-xsync-tool=ngamsUtils.ngasXSyncTool:main'
        ],
    }
)

