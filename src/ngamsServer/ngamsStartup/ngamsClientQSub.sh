#!/bin/bash

#PBS -l select=40:ncpus=12:mem=48gb:mpiprocs=1
#PBS -l walltime=03:00:00
#PBS -m e
#PBS -q workq
#PBS -N NGAS_40_client
#PBS -A cwu-icrar
#PBS -W group_list=astronomy564
#PBS -o /home/cwu/ngas_client_run/client_run.out
#PBS -e /home/cwu/ngas_client_run/client_run.err

#source /home/cwu/dc_env.sh
OBS_ID=$(date +%s)
mpirun -np 40 /home/cwu/mwa/ngas_rt/bin/python /home/cwu/mwa/ngas_rt/src/ngamsStartup/ngamsClientFornaxMgr.py -o $OBS_ID -d 512 -s 40 -n -g 