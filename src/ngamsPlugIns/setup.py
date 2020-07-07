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
from setuptools import setup, find_packages

with open('../../VERSION') as vfile:
    for line in vfile.readlines():
        if "ngamsNGAMS_SW_VER" in line:
            version = line.split("NGAMS_SW_VER ")[1].strip()[1:-1]
            break

setup(
    name='ngamsPlugIns',
    version=version,
    description="The base packages that make up the NGAMs system",
    long_description="The base modules that make up the NGAMs system, namely the ngamsLib, ngamsData and ngamsSql packages",
    classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='',
    author_email='',
    url='',
    license='',
    # packages=find_packages(exclude='contributed/*'),
    packages=find_packages(),
    include_package_data=True,
    package_data = {
        'ngamsPlugIns': ['README', '*.css', '*.html', '*.cfg', 'd3/*', 'subscription_ui/*']
        # 'ngamsPlugIns' : ['README', '*.css', '*.html', '*.cfg', 'd3/*', 'mwa_compression/*',
        #                   'mwa_gleam/ngamsPlugins/*', 'mwa_gleam/rdsi/*', 'mwa_gleam/vo/*',
        #                   'mwa_gleam/*.sh', 'mwa_gleam/*.sql', 'mwa_gleam/*.py', 'mwa_mit/*', 'mwa_tapestaging/*',
        #                   'subscription_ui/*'],
    },
    install_requires=[
#        'astropy',
        'ngamsCore',
        'ngamsPClient',
        'ngamsServer'
    ]
)
