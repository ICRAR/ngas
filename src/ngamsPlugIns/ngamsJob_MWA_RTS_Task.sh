#!/bin/bash
# NGAS processing job is executed by an ngas server as an external (newly created) process

# We want Fornax (with its local storage, i.e. 7TB per computer node) 
# to be a temporary data storage for data processing.  
# To achieve that, we use NGAS to 
# (1) manage data staging from Cortex to Fornax, 
# (2) manage those local disks for staged files and image files (if archived back to ngas)
# (3) interleave staging and RTS processsing to increase I/O-compute parallelsim 
#		either across multiple observations or even within a single observation
#
# Who       			When          What
# -----------------   ----------    ---------
# chen.wu@icrar.org   15-May-2012    created
 

# Get correct modules and paths
source /scratch/astronomy556/MWA/ngas_rt/src/ngamsPlugIns/ngamsJob_MWA_RTS_Task_Env.sh

# Activate the python virtual environment
source /scratch/astronomy556/MWA/bin/activate 

# Point to the right NGAS database 
# this could be skipped if we have a correct 'mwa.conf' at home directory
# change_db.py curtin

# invoke RTS with all existing paramters untouched
python /scratch/astronomy556/MWA/ngas_rt/src/ngamsPlugIns/ngamsJob_MWA_RTS_Task.py $@