from setuptools import setup, find_packages
import sys, os

version = '0.1'

from pkg_resources import Requirement, resource_filename
from glob import glob

data = glob('ngamsData/*')

setup(name='ngamsPClient',
      version=version,
      description="'NGAS Python Client'",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Andreas Wicenec',
      author_email='awicenec@gmail.com',
      url='',
      license='LGPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      data_files=[('doc', ['doc/COPYRIGHT', 'doc/VERSION', 'doc/ngamsPClient.doc']),
                  ('ngamsData', data),
                  ('ngamsLib',['README']),
                  ('.',['command_line.py', 'ngamsPClient.py']),
                  ],
#      scripts = ['ngamsPClient.py'],
      include_package_data=True,
      zip_safe=False,
      entry_points= {
      'console_scripts':[
      'ngamsPClient = command_line:main'
      ],
      },
      )
