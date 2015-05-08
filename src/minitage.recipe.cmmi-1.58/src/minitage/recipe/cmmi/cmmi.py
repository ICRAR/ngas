# Copyright (C) 2009, Mathieu PASQUET <kiorky@cryptelium.net>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. Neither the name of the <ORGANIZATION> nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.


import os
import shutil
import logging
try:
    from hashlib import sha1
except ImportError: # Python < 2.5
    from sha import new as sha1
try:
    from os import uname
except:
    from platform import uname

from distutils.dir_util import copy_tree
from minitage.recipe.common import common
from minitage.core.common import  splitstrip, remove_path
from minitage.core import core

def move_lafiles(directory, logger=False):
    if not logger:
        logging.basicConfig()
        logger = logging.getLogger('cmmi.remove_lafiles')
    p = os.path.abspath(directory)
    if os.path.exists(p):
        for cwd, ds, fs in os.walk(p):
            for f in fs:
                fp = os.path.join(cwd, f)
                if fp.endswith('.la'):
                    bfp = "%s.old" % fp
                    if os.path.exists(bfp):
                        try:
                            remove_path("%s.old" % fp)
                        except:
                            pass
                    try:
                        os.rename(fp, "%s.old" % fp)
                    except:
                        logger.error('Cant remove Libtool Archive: %s' % fp)

class Recipe(common.MinitageCommonRecipe):
    """zc.buildout recipe for compiling and installing software"""

    def __init__(self, buildout, name, options):

        common.MinitageCommonRecipe.__init__(self,
                                             buildout,
                                             name,
                                             options)
        # handle share mode, compatibility with zc.recipe.cmmi
        self.shared = False
        self.shared_top = os.path.join(self.download_cache, 'cmmi')
        if not os.path.isdir(self.shared_top):
                os.makedirs(self.shared_top)


        # configure script for cmmi packages
        self.configure = options.get('configure-%s' % self.osxflavor, None)
        if not self.configure:
            self.configure = options.get('configure-%s' % self.uname.lower(), None)
        if not self.configure:
            self.configure = options.get('configure', 'configure')

        # prefix separtor in ./configure --prefix%SEPARATOR%path
        self.prefix_separator = options.get('prefix-separator', '=')
        if self.prefix_separator == '':
            self.prefix_separator = ' '
        self.prefix_option = self.options.get(
            'prefix-option',
            '--prefix%s' % self.prefix_separator)

        # configuration options
        self.move_lafiles = self.options.get('move-lafiles', '').strip()
        self.autogen = self.options.get('autogen', '').strip()
        self.configure_options = ' '.join(
            splitstrip(
                self.options.get( 'configure-options', '')
            )
        )
        self.configure_options += ' %s ' % ' '.join(
            splitstrip(
                self.options.get( 'extra_options', '')
            )
        )
        # compatibility with zc/recipe.cmmi
        self.extra_options = self.configure_options
        self.patch = self.patch_cmd
        # conditionnaly add OS specifics patches.
        self.configure_options += ' %s' % (
            self.options.get('configure-options-%s' % (self.uname.lower()), '')
        )

        # configure options per os
        configoptreplacer = self.options.get(
            'configure-options-replace-%s' % self.uname.lower(),
            ''
        ).strip()
        if configoptreplacer:
            self.configure_options = ' %s' % (configoptreplacer)
        else:
            configoptreplacer = self.options.get(
                'configure-options-%s-replace' % self.uname.lower(),
                ''
            ).strip()
            if configoptreplacer:
                self.configure_options = ' %s' % (configoptreplacer)




        if 'darwin' in self.uname.lower():
            kv = uname()[2]
            osxflavor = None
            if kv == '9.8.0':
                osxflavor = 'leopard'
            if kv.startswith('10.'):
                osxflavor = 'snowleopard'
            if osxflavor:
                self.configure_options += ' %s' % self.options.get('configure-options-%s' % osxflavor, '')

        self.configure_options = common.ENDLINE.sub(' ', self.configure_options)

        # if gmake is setted. taking it as the make cmd !
        # be careful to have a 'gmake' in your path
        # we have to make it only in non linux env.
        # if wehave gmake setted, use gmake too.
        gnumake = 'make'
        if (
            self.options.get('gmake', 
                self.buildout.get('part', {}).get('gmake', None)
            )
            and self.uname not in ['cygwin', 'linux']
        ):
            gnumake = 'gmake'
        self.options['make-binary'] = self.make_cmd = self.options.get('make-binary-%s'%self.uname,
                                        self.options.get('make-binary', gnumake)).strip()
        self.options['make-options'] = self.make_options = self.options.get('make-options-%s'%self.uname,
                                                 self.options.get('make-options', '')).strip()

        # what we will install.
        # if 'make-targets'  present, we get it line by line
        # and all target must be specified
        # We will default to make '' and make install
        self.install_in_place = self.options.get('install-in-place')
        self.makedir = self.options.get('makedir-%s'%self.uname, self.options.get('makedir', '')).strip()
        self.makeinstalldir = self.options.get('makeinstalldir-%s'%self.uname,
                                               self.options.get('makeinstalldir','')).strip()
        self.make_targets = splitstrip(
            self.options.get('make-targets-%s'%self.uname,
                             self.options.get( 'make-targets', ' ')),
            '\n'
        )
        if not self.make_targets:
            self.make_targets = ['']


        self.make_install_append_options = self.options.get('make-install-append-options%s'%self.uname,
                                                self.options.get('make-install-append-options', '')).strip()
        self.install_options = self.options.get('make-install-options%s'%self.uname, self.options.get('make-install-options', ''))
        self.install_targets =  ['%s %s' % (a, self.install_options)
                                 for a in splitstrip(
                                     self.options.get(
                                         'make-install-targets%s'%self.uname,
                                         self.options.get( 'make-install-targets', 'install')),
                                     '\n'
                                 )]

        # shared builds
        if 'shared' in self.options:
            self.shared = os.path.join(
                self.shared_top,
                self._state_hash()
            )
            self.prefix = options['location'] = self.shared

        # 'no' options
        self.noconfigure = options.get('noconfigure-%s'%self.uname,
                                        options.get('noconfigure', ''))
        self.nomake = options.get('nomake-%s'%self.uname,
                                  options.get('nomake', ''))
        self.noinstall = options.get('noinstall-%s'%self.uname,
                                     options.get('noinstall', ''))

    def install(self):
        """Install the recipe."""
        # initialise working directories
        for path in [self.prefix, self.tmp_directory]:
            if not os.path.exists(path):
                os.makedirs(path)
        try:
            cwd = os.getcwd()
            # downloading or get the path
            # in the cache if we are offline
            fname = self._download(md5=self.md5, cache=True)

            # preconfigure hook
            self._call_hook('pre-unpack-hook')

            # unpack
            self._unpack(fname)

            # get default compilation directory
            self.compil_dir = self._get_compil_dir(self.tmp_directory)
            if self.inner_dir:
              self.compil_dir = self._get_compil_dir(self.inner_dir)

            # set path
            self._set_path()

            # set pkgconfigpath
            self._set_pkgconfigpath()

            # set compile path
            self._set_compilation_flags()

            # set pypath
            self._set_py_path()

            # write environment file for further debugging
            self.write_env()
            # preconfigure hook
            self._call_hook('post-unpack-hook')

            # configure compilation build directory
            if self.build_dir:
                if not os.path.isdir(self.build_dir):
                    os.makedirs(self.build_dir)
            else:
                self.build_dir = self.compil_dir

            # apply patches
            self._patch(self.build_dir)

            # preautogen hook
            self._call_hook('pre-autogen-hook')

            # autogen, maybe
            self._autogen()

            # choose configure
            self.configure = self._choose_configure(self.compil_dir)
            self.options['compile-directory'] = self.build_dir

            # preconfigure hook
            self._call_hook('pre-configure-hook')

            # run configure
            self._configure(self.configure)

            # postconfigure/premake hook
            self._call_hook('pre-make-hook')

            # running make
            #import pdb;pdb.set_trace()
            self._make(self.build_dir, self.make_targets)

            # post build hook
            self._call_hook('post-build-hook')

            # installing
            self._make_install(self.build_dir)

            # post hook
            self._call_hook('post-make-hook')

            # cleaning
            if 'debug' in self.options:
                import pdb;pdb.set_trace()
            os.chdir(cwd)
            for path in self.build_dir, self.tmp_directory:
                if os.path.isdir(path):
                    shutil.rmtree(path)

            # regaining original cwd in case we changed build directory
            # during build process.

            # remove lafiles
            if self.move_lafiles:
                move_lafiles(self.prefix)

            self.logger.info('Completed install.')
        except Exception, e:
            raise
            self.logger.error('Compilation error. '
                              'The package is left as is at %s where '
                      'you can inspect what went wrong' % self.tmp_directory)
            self.logger.error('Message was:\n\t%s' % e)
            raise core.MinimergeError('Recipe failed, cant install.')

        return []

    def _state_hash(self):
        # hash of our configuration state, so that e.g. different
        # ./configure options will get a different build directory
        env = ''.join(['%s%s' % (key, value) for key, value
                       in self.environ.items()])
        state = [self.url, self.extra_options, self.autogen,
                 self.patch, self.patch_options, env]
        return sha1(''.join(state)).hexdigest()

    def _autogen(self):
        """Run autogen script.
        """
        self.go_inner_dir()
        cwd = os.getcwd()
        os.chdir(self.build_dir)
        if 'autogen' in self.options:
            self.logger.info('Auto generating configure files')
            autogen = os.path.join(self.build_dir, self.autogen)
            self._system(autogen)
        os.chdir(cwd)

    def _choose_configure(self, compile_dir=None):
        """configure magic to runne with
        exotic configure systems.
        """
        self.go_inner_dir()
        configure = os.path.join(compile_dir, self.configure)
        if not os.path.isfile(configure) \
           and (not self.noconfigure):
            self.logger.error('Unable to find the configure script')
            raise core.MinimergeError(
                'Invalid package contents, '
                'there is no configure script in %s.' % compile_dir
            )
        return configure

    def _configure(self, configure):
        """Run configure script.
        Argument
            - configure : the configure script
        """
        self.go_inner_dir()
        cwd = os.getcwd()
        os.chdir(self.build_dir)
        if not self.noconfigure:
            self._system(
                    '%s %s%s %s' % (
                        configure,
                        self.prefix_option,
                        self.prefix,
                        self.configure_options
                    )
                )
        os.chdir(cwd)

    def _make(self, directory, targets):
        """Run make targets except install."""
        self.go_inner_dir()
        cwd = os.getcwd()
        os.chdir(directory)
        if self.makedir and os.path.exists(self.makedir):
            os.chdir(self.makedir)
        if not self.nomake:
            for target in targets:
                try:
                    self._system('%s %s %s' % (self.make_cmd, self.make_options, target))
                except Exception, e:
                    message = 'Make failed for targets: %s' % targets
                    raise core.MinimergeError(message)
        os.chdir(cwd)

    def _make_install(self, directory):
        """"""
        # moving and restoring if problem :)
        self.go_inner_dir()
        cwd = os.getcwd()
        os.chdir(directory)
        if self.makeinstalldir:
            os.chdir(self.makeinstalldir)
        tmp = '%s.old' % self.prefix
        if len(self.install_options)>0:
            self.make_options = self.install_options
        self.make_options_before = self.make_options
        self.make_options_after = ''
        if len(self.make_install_append_options) > 0:
            self.make_options_before = ''
            self.make_options_after = self.make_options
        self.make_options = ''
        self.install_targets = ['%s %s %s' % (self.make_options_before,
                                              t,
                                              self.make_options_after)
                                for t in self.install_targets]
        self.make_options = ''
        if not self.noinstall:
            if not os.path.exists(tmp):
                os.makedirs(tmp) 
            if os.path.isdir(self.prefix):
                copy_tree(self.prefix, tmp)
            if not self.install_in_place:
                os.chdir(cwd)
                shutil.rmtree(self.prefix)
            try:
                if not os.path.exists(self.prefix):
                    os.makedirs(self.prefix)
                self._call_hook('pending-make-install-hook')
                self._make(directory, self.install_targets)
            except Exception, e:
                remove_path(self.prefix)
                if os.path.exists(tmp):
                    shutil.move(tmp, self.prefix)
                raise core.MinimergeError('Install failed:\n\t%s' % e)
        if 'debug' in self.options:
            self.logger.debug('Make INSTALL STOP')
            import pdb;pdb.set_trace()
        if os.path.exists(tmp):
            shutil.rmtree(tmp)
        os.chdir(cwd)
