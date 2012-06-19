"""
Generic Test case
"""
__docformat__ = 'restructuredtext'

import unittest
import doctest
import sys
import os
import shutil
import popen2
import subprocess
import virtualenv as m_virtualenv
from zc.buildout.testing import *
from os import makedirs
from shutil import copy
from zc.buildout.buildout import Buildout
from zc.buildout import buildout as bo
from zc.buildout.testing import start_server, _start_server, stop_server
from setuptools.package_index import PackageIndex

def get_uname():
    if 'linux' in sys.platform:
        return 'linux'
    else:
        return sys.platform
uname = get_uname()

def get_args(args):
    res = []
    for arg in args:
        if isinstance(arg, str):
            res.append(arg)
        if isinstance(arg, list) or isinstance(arg, tuple):
            res.extend(get_args(arg))
    return res


def get_joined_args(args):
    res = get_args(args)
    return os.path.join(*res)


current_dir = os.path.abspath(os.path.dirname(__file__))
def mkdir(*args):
    a = get_joined_args(args)
    if not os.path.isdir(a):
        makedirs(a)

def rmdir(*args):
    a = get_joined_args(args)
    if os.path.isdir(a):
        shutil.rmtree(a)

def sh(cmd, in_data=None):
    _cmd = cmd
    print cmd
    p = subprocess.Popen([_cmd], shell=True,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         close_fds=True)

    if in_data is not None:
        p.stdin.write(in_data)

    p.stdin.close()

    print p.stdout.read()
    print p.stderr.read()

def ls(*args):
    a = get_joined_args(args)
    if os.path.isdir(a):
        filenames = os.listdir(a)
        for filename in sorted(filenames):
            print filename
    else:
        print 'No directory named %s' % args

def cd(*args):
    a = get_joined_args(args)
    os.chdir(a)

def config(filename):
    return os.path.join(current_dir, filename)

def install_eggs_from_pathes(reqs, pathes=None, path='eggs'):
    if pathes:
        env = pkg_resources.Environment()
        ws = pkg_resources.WorkingSet()
        rs=[]
        for req in reqs:
            rs.append(pkg_resources.Requirement.parse(req))

        dists = ws.resolve(rs, env)
        to_copy=[]
        for dist in dists:
            if dist.precedence != pkg_resources.DEVELOP_DIST:
                to_copy.append(dist)

        for dist in to_copy:
            r = dist.as_requirement()
            install('%s'%r, path)


def install_develop_eggs(develop_eggs=None, develop_path='develop-eggs'):
    if develop_eggs:
        for d in develop_eggs:
            install_develop(d, develop_path)


def cat(*args, **kwargs):
    filename = os.path.join(*args)
    if os.path.isfile(filename):
        data = open(filename).read()
        if kwargs.get('returndata', False):
           return data
        print data
    else:
        print 'No file named %s' % filename

def touch(*args, **kwargs):
    filename = os.path.join(*args)
    open(filename, 'w').write(kwargs.get('data',''))


def virtualenv(path='.', args=None):
    if not args:
        args = ['--no-site-packages']
    argv = sys.argv[:]
    sys.argv = [sys.executable] + args + [path]
    m_virtualenv.main()
    sys.argv=argv


def virtualenv(path='.', args=None):
    if not args:
        args = ['--no-site-packages']
    argv = sys.argv[:]
    sys.argv = [sys.executable] + args + [path]
    m_virtualenv.main()
    sys.argv=argv


def buildout(*args):
    argv = sys.argv[:]
    sys.argv = ["foo"] + list(args)
    ret = bo.main()
    sys.argv=argv
    return ret

#execdir = os.path.abspath(os.path.dirname(sys.executable))
tempdir = ['/tmp', 'buildout.test']

def buildoutSetUp(test):
    test.globs['__tear_downs'] = __tear_downs = []
    test.globs['register_teardown'] = register_teardown = __tear_downs.append
    def start_server_wrapper(path):
        port, thread = _start_server(path, name=path)
        url = 'http://localhost:%s/' % port
        register_teardown(lambda: stop_server(url, thread))
        return url
    test.globs.update(dict(
        start_server = start_server_wrapper
    ))

def buildoutTearDown(test):
     for f in test.globs['__tear_downs']:
        f()

def doc_suite(test_dir, setUp=None, tearDown=None, globs=None):
    """Returns a test suite, based on doctests found in /doctest."""
    suite = []
    bp = os.path.dirname(sys.argv[0])
    os.environ['PATH'] = ":".join(
        ["/tmp/buildout.test/bin/",
        bp,
        os.environ.get('PATH', '')]
    )

    if globs is None:
        globs = globals()
        globs["bp"] = bp
        globs["p"] = os.path.dirname(bp)
        #globs["buildout"] = os.path.join(bp, 'buildout')
        globs["python"] = os.path.join(bp, 'py')
    flags = (doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE |
             doctest.REPORT_ONLY_FIRST_FAILURE)

    package_dir = os.path.split(test_dir)[0]
    if package_dir not in sys.path:
        sys.path.append(package_dir)

    doctest_dir = test_dir

    # filtering files on extension
    docs = [os.path.join(doctest_dir, doc) for doc in
            os.listdir(doctest_dir) if doc.endswith('.txt')
           ]

    for ftest in docs:
        test = doctest.DocFileSuite(ftest, optionflags=flags,
                                    globs=globs, setUp=setUp,
                                    tearDown=buildoutTearDown,
                                    module_relative=False)

        suite.append(test)
    return unittest.TestSuite(suite)

def test_suite():
    """returns the test suite"""
    return doc_suite(current_dir,
                    setUp = buildoutSetUp,
                    tearDown = buildoutTearDown)

if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')

