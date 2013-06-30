#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2012
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

# Fornax module for MWA RTS
#
# Who                   When          What
# -----------------   ----------    ---------
# chen.wu@icrar.org   15-May-2013    created

"""
rts executable that will be invoked by rts_run.sh to produce images on each Fornax compute node
This is based on the new version of RTS "direct-from-correlator-files" that does not generate lfile and uvfits
the executable also needs to produce some meeta data before using rts

Some constraints for scheduler:
1. one node can run one rts_executable at a time since GPU access is mutually exclusive
    1.1. To support this, ngas job plugin accepts a parameter 'exclusive'
2. The final outcome signal (success or failure) must report back to the rts_job_mgr.py

Either the job itself or the job wrapper should do the additional tasks:
3. The image result will be archived back to the NGAS node under a different storage set (volume)
    3.1 Before being archived, the result must be zipped into a single file, whose id is: job_id_obs_id_corr_id.tar.gz
"""
import commands, os, fnmatch
from optparse import OptionParser

mwa_path = '/scratch/astronomy556/MWA'
ngas_path = '/tmp/rts_processing'
ngas_src = '/scratch/astronomy556/MWA/ngas_rt'
ngas_cclient = '%s/bin/ngamsCClient' % ngas_src
_START_128T = 1046304456 # hard-coded knowledge

# this can be passed in as an optional parameter 'rts_tpl_pf'
# so that users can use their own templates, trying different parameters with RTS.
rts_tpl_pf = '%s/RTS/utils/templates/RTS_template_' % mwa_path 
rts_tpl_sf = '.in'
time_resolution = 0.5 # under 32T, this was 1
fine_channel = 40 # KHz, under 128T, this was 10
num_subband = 24 # should be the same as coarse channel
tiles = '128T'
use_gpu = 1
added_lines = ['UseCorrelatorInput=1', 'UsePacketInput=0', 'UseThreadedVI=0',
               'CorrelatorPort=65535', 'ArrayFile=array_file.txt',
               'ArrayPositionLat=-26.70331940',
               'ArrayPositionLong=116.67081524']
added_lines128T = ['ChannelBandwidth=0.04', 'ArrayNumberOfStations=128',
               'NumberOfChannels=32']
added_lines32T = ['ChannelBandwidth=0.01', 'ArrayNumberOfStations=32',
               'NumberOfChannels=1536']
"""
-- add the following ---
UseCorrelatorInput=1
UsePacketInput=0
UseThreadedVI=0
CorrelatorPort=65535
ArrayFile=array_file.txt
ChannelBandwidth=0.04
ArrayNumberOfStations=128
NumberOfChannels=32
ArrayPositionLat=-26.70331940
ArrayPositionLong=116.67081524

"""

def execCmd(cmd, failonerror = True):
    re = commands.getstatusoutput(cmd)
    if (failonerror and re[0]):
        print('Fail to execute: "%s". Exception: %s' % (cmd, re[1]))
        exit(re[0] % 255 + 1) # the bash exit code range is 0 - 255, but it should not be zero in this case so 1 - 255
    return re

def parseOptions():
    """
    Obtain the following parameters
    obs_num:       the observation number.                  
   
    rts-tpl-pf:    RTS template prefix (optional, string, 
                   default = '/scratch/astronomy556/MWA/RTS/utils/templates/RTS_template_')
                   
    rts-tpl-sf:    RTS template suffix (optional, string, default = '.in')
    
    rts-tpl:       RTS templates aliases for this processing (optional, string - comma separated aliases, e.g.
                   drift,regrid,snapshots, default = 'regrid'). Each alias will be concatenated with 
                   rts-tpl-pf and rts-tpl-sf to form the complete template file path, 
                   e.g. /scratch/astronomy556/MWA/RTS/utils/templates/RTS_template_regrid.in
    """
    parser = OptionParser()
    parser.add_option("-j", "--job", dest = "job_id", help = "NGAS Job Id (mandatory)")
    parser.add_option("-o", "--obs", dest = "obs_num", help = "Observation number (mandatory, not a GPS range")
    parser.add_option("-c", "--corrid", dest = "corr_id", help = "Correlator Id")
    parser.add_option("-p", "--tplpf", dest = "tpl_pf", help = "RTS template prefix (optional)")
    parser.add_option("-s", "--tplsf", dest = "tpl_sf", help = "RTS template suffix (optional)")
    parser.add_option("-t", "--tpl", dest = "rts_tpl", help = "RTS template names, comma separated (e.g. regrid, drift) (optional)")      
    parser.add_option("-f", "--filelist", dest = "file_list", help = "A list of local files separated by comma (mandatory)")
    parser.add_option("-g", "--gpu", dest = "use_gpu", help = "Y - use gpu, N - use cpu (optional)")
    #parser.add_option("-n", "--ngas", dest = "ngas_host", help = "The local NGAS host:port")

    (options, args) = parser.parse_args()
    if (None == options.obs_num):
        #parser.print_help()
        print 'Observation number is None'
        return None
    return options

def main():
    """
    Program entry
    """
    opts = parseOptions()
    if (not opts):
        exit(1)

    global rts_tpl_pf, rts_tpl_sf, time_resolution, fine_channel, tiles
    obs_num = int(opts.obs_num)
    if (obs_num < _START_128T):
        time_resolution = 1
        fine_channel = 10
        tiles = '32T'
    
    # create a working directory
    corrId = int(opts.corr_id)
    if (corrId > 9):
        gpubox = '%d' % corrId
    else:
        gpubox = '0%d' % corrId
    work_dir = '%s/%s/%d/gpubox%s' % (ngas_path, opts.job_id, obs_num, gpubox)
    if (os.path.exists(work_dir)):
        execCmd('rm -rf %s' % work_dir)
        
    cmd = 'mkdir -p %s' % work_dir
    execCmd(cmd)
        
    # change to working directory, i.e. cd $work_dir
    os.chdir(work_dir)
    
    # create symbolic links in the work_dir to real file paths 
    fileList = opts.file_list.split(',')
    for file in fileList:
        base_name = os.path.basename(file)
        cmd = 'ln -s %s %s' % (file, base_name)
        execCmd(cmd) #don't care if this failed
    
    # create the template list file
    if (opts.tpl_pf):
        rts_tpl_pf = opts.tpl_pf
    if (opts.tpl_sf):
        rts_tpl_sf = opts.tpl_sf
    
    if (opts.rts_tpl):
        temp_alias = opts.rts_tpl.split(',')
    else:
        temp_alias = ['regrid']
    
    templates = []
    for tpl in temp_alias:
        template = '%s%s%s' % (rts_tpl_pf, tpl, rts_tpl_sf)
        if (os.path.exists(template)):
            templates.append(template)
            execCmd('mkdir %s' % tpl)
    
    f = open('rts_template_list.dat', 'w')
    try:
        for tplt in templates:
            f.write(tplt + '\n')
    except Exception, err:
        print 'Fail to add template file %s: %s' % (tplt, str(err))
        exit(1)
    finally:
        if (f): 
            f.close()  
            
    # make_metafiles
    cmd = 'make_metafiles.py -l --gps=%d --dt %s --df %d --header=header.txt --antenna=antenna_locations.txt --instr=instr_config.txt --rts' \
        % (obs_num, str(time_resolution), fine_channel)
    execCmd(cmd)
    
    # generate rts input files
    cmd = 'generate_RTS_in.py %s ngas_run 24 --templates=rts_template_list.dat %s' \
        % (work_dir, tiles)
    execCmd(cmd)
    
    # update the rts input file
        # 1. change the BaseFilename for all input files
    cmd = 'sed -i s/ngas_run_band_/*gpubox*/g ngas_run_rts_*.in'
    execCmd(cmd)
        # 2. add correct subbandId
    cmd = 'sed -i s/SubBandIDs=/SubBandIDs=%d/g ngas_run_rts_*.in' % (corrId)
    execCmd(cmd)
        # 3. compute the ObservationTimeBase
    cmd = 'timeconvert.py --gps=%d' % obs_num
    re = execCmd(cmd)
    obstimebase_line = 'ObservationTimeBase=%s' % re[1].split('\n')[5].split()[1]
        # 4. append to all input files
    lines = [obstimebase_line] + added_lines
    if ('128T' == tiles):
        lines += added_lines128T
    else:
        lines += added_lines32T
    
    for i in range(len(templates)):
        fn = 'ngas_run_rts_%d.in' % i
        f = open(fn, 'a')
        try:
            for line in lines:
                f.write('\n' + line)
        except Exception, err:
            print 'Fail to add lines to rts input file %s: %s' % (fn, str(err))
            exit(1)
        finally:
            if (f):
                f.close()
    
    # for each template, run rts, and move results to sub-dir
    for i in range(len(templates)):
        fn = 'ngas_run_rts_%d.in' % i    
        if (opts.use_gpu == 'N'):
            cmd = 'rts_node_cpu %s' % fn
        else:
            cmd = 'rts_node_gpu %s' % fn
        execCmd(cmd)
        
        #move all produced image fits files to the template dir
        if (not os.path.exists(temp_alias[i])):
            cmd = 'mkdir %s' % temp_alias[i]
            execCmd(cmd)
        moveAllImgFiles(temp_alias[i])
        #cmd = 'mv %s*.fits %s/' % (obstimebase_line.split('=')[1].split('.')[0], temp_alias[i])
        #execCmd(cmd)
        
        #move all dat files
        cmd = 'mv BandpassCalibration*.dat %s/' % temp_alias[i]
        execCmd(cmd, False)
        cmd = 'mv DI_JonesMatrices*.dat %s/' % temp_alias[i]
        execCmd(cmd, False)
        
        #move log files           
        moveLogFiles(corrId, temp_alias[i])
        #cmd = 'mv rts*.log %s/' % temp_alias[i]
        #execCmd(cmd)
        
        # move flagged channles
        cmd = 'mv flagged_channels.txt %s/flagged_channels_node%d.txt' % (temp_alias[i], corrId)
        execCmd(cmd, False)
        
        # copy configuration/array pos files for future references
        cmd = 'cp *.txt %s/' % temp_alias[i]  
        execCmd(cmd, False)
    
    # pack all template directories into a single 'image (gzip)' file   
    imgFile = '%s__%d__%s.tar.gz' % (opts.job_id, obs_num, gpubox)
    cmd = 'tar -czf %s' % imgFile
    for tpa in temp_alias:
        cmd += ' %s/' % tpa
    execCmd(cmd)
    
    # output the local path to the final image(gzip) file
    print '%s/%s' % (work_dir, imgFile)
    exit(0)
    
    """
    # archive this zip file locally and print the retrieve url for the image file as the output  
    # Task per se no longer archives file, this is done by the framework
    cmd = '%s -mimeType application/octet-stream -servers %s -cmd QARCHIVE -fileUri %s/%s' % (ngas_cclient, opts.ngas_host, work_dir, imgFile)
    execCmd(cmd)
    print 'http://%s/RETRIEVE?file_id=%s' % (opts.ngas_host, imgFile)
    """

def moveAllImgFiles(tgtPath):
    """
    Move newly generated image files (FITS) from current directory 
    to the target directory
    Do not move the original visibility FITS files, which are all 
    symbolic links
    
    tgtPath:    destination directory 
    """
    for file in os.listdir('.'):
        if (fnmatch.fnmatch(file, '*.fits') and 
            (not os.path.islink(file))):
            os.rename(file, '%s/%s' % (tgtPath, file))
    
def moveLogFiles(corrId, tgtPath):
    """
    The rts_node_gpu/cpu seems always to produce log file with a name suffix 'node001.log'
    this function changes node001 to the node$(corrid)
    
    corrid:    correlator (gpubox) id (int)
    """
    if (corrId > 9):
        node = 'node0%d' % corrId
    else:
        node = 'node00%d' % corrId
    
    for file in os.listdir('.'):
        if fnmatch.fnmatch(file, 'rts*.log'):
            #first, change the file name from node001 to node020 (for example)
            newfn = file.replace('node001', node)
            os.rename(file, '%s/%s' % (tgtPath, newfn))

if __name__ == "__main__":
    main()

"""
# make_metafiles.py -l --gps=1049200392 --dt 0.5 --df 40 --header=header.txt --antenna=antenna_locations.txt --instr=instr_config.txt --rts
# generate_RTS_in.py $PWD ngas_run 24 --templates=/scratch/astronomy556/bpindor/MWA_test/rts_template_list.dat 128T
# check source list is correct? (main thing is the template)
# get observationTimeBase:
"""
"""
timeconvert.py --gps=1049200392
"""
# rts_node_gpu ngas_run_rts_0.in
 # find . -name *rts_0.in | xargs grep SubBandID

# content in ngas_run_rts_0.in
"""
-- change to this ---
BaseFilename=/tmp/rts_test/*gpubox*
ObservationTimeBase=2456388.022870



-- uncomment --
# but this is set differently for different templates
RegridMethod=2
DoRegriddingWithProjection=1024

"""
