'''
Created on Sep 19, 2012

@author: awicenec
'''
#
#    (c) University of Western Australia
#    International Centre of Radio Astronomy Research
#    M468/35 Stirling Hwy
#    Perth WA 6009
#    Australia
#
#    Copyright by UWA,
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

import argparse, subprocess, os, uuid, itertools
import time

def _createFile(name):
    """
    Create a file with <name>
    INPUTS:    
        name:       string, the name of the file
    
    RETURNS:    
        None
    """
    subprocess.call(["touch", name])
    return


def generateFilesDirs(top, ndirs, nfils):
    """
    Generate N directories containing M files each.
    INPUTS:    
        top:         string, the top-level directory
        ndirs:       int, Number of directories to generate
        nfils:       int, Number of files to generate
    
    RETURNS:    
        Duration in seconds to generate the files
    """
    dirFiles = itertools.product(range(1,ndirs+1,1),range(1,nfils+1,1))
    dir = 0
    st = time.time()
    for dirFile in dirFiles:
        if dir != dirFile[0]:
            dir = dirFile[0]
            os.mkdir(top + '/' + str(dir))
        _createFile('/'.join([top,str(dir),str(dirFile[1])]))
    dur = time.time() - st
    
    return dur

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate directories '
    'and files and delete them again to test FS performance.')
    parser.add_argument('ndirs', metavar='N', type=int,
                        default=100,
                       help='Number of directories')
    parser.add_argument('nfils', metavar='N', type=int,
                        default=100,
                       help='Number of files per directory')
    
    args = parser.parse_args()
    print "Generating %d files in %d directories" % (args.nfils, args.ndirs)
    top = 'tmp' + str(uuid.uuid1())
    os.mkdir(top)

    dur = generateFilesDirs(top, args.ndirs, args.nfils)
    print "Generation of %d files took %5.2f seconds" % (args.ndirs*args.nfils, dur)
    st = time.time()
    subprocess.call(["rm", "-rf", top])
    dur = time.time() - st
    print "Removal of %d files took %5.2f seconds" % (args.ndirs*args.nfils, dur)

