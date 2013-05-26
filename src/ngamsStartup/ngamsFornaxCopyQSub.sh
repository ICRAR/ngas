#!/bin/bash

#PBS -l select=1:ncpus=1:mem=8gb:mpiprocs=1
#PBS -l walltime=00:60:00
#PBS -m e
#PBS -q copyq
#PBS -N NGAS_IO_cluster
#PBS -A cwu-icrar
#PBS -W group_list=astronomy556
#PBS -o /home/cwu/ngas_run/run_io.out
#PBS -e /home/cwu/ngas_run/run_io.err

mpirun -np 1 /scratch/astronomy556/MWA/ngas_rt/bin/python /scratch/astronomy556/MWA/ngas_rt/src/ngamsStartup/ngamsFornaxMgr.py