import os, sys
from setuptools import setup, find_packages
setupdir = os.path.abspath(
    os.path.dirname(__file__)
)
os.chdir(setupdir)

name='minitage.recipe.cmmi'
version = '1.58'

def read(*rnames):
    return open(
        os.path.join(setupdir, *rnames)
    ).read()

long_description = (
    read('README.txt')
    + '\n'
    + read('src', 'minitage', 'recipe', 'cmmi', 'tests', 'cmmi.txt')
    + '\n'
    + read('CHANGES.txt')
    + '\n'
)

if 'RST_TEST' in os.environ:
    print long_description
    sys.exit(0)

setup(
    name=name,
    version=version,
    description="zc.buildout recipes to compile and install software or python packages and generate scripts or configuration files  sponsored by Makina Corpus.",
    long_description= long_description,
    classifiers=[
        'Framework :: Buildout',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Topic :: Software Development :: Build Tools',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    keywords='development buildout recipe',
    author='Mathieu Pasquet',
    author_email='kiorky@cryptelium.net',
    url='http://cheeseshop.python.org/pypi/%s' % name,
    license='BSD',
    packages=find_packages('src'),
    package_dir = {'': 'src'},
    namespace_packages=['minitage', 'minitage.recipe', 'minitage.recipe.cmmi'],
    include_package_data=True,
    zip_safe=False,
    install_requires = [
        'zc.buildout',
        
        'minitage.core',
        'iniparse',
        'minitage.recipe.common',
    ],
    extras_require={'test': ['IPython', 'zope.testing', 'mocker']},
    #tests_require = ['zope.testing'],
    #test_suite = '%s.tests.test_suite' % name,
    # adding zdu, setuptools seems to order recipes executions
    # in akphabetical order for entry points
    # workaround when using the 2 recipes in the same buildout.
    entry_points = {
        'zc.buildout' : [
            'cmmi = %s:Recipe' % 'minitage.recipe.cmmi.cmmi',
            'default = %s:Recipe' % 'minitage.recipe.cmmi.cmmi',
        ]
    },
)


