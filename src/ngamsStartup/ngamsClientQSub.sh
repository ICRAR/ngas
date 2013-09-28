#!/bin/bash

#PBS -l select=24:ncpus=12:ngpus=1:mem=2gb:mpiprocs=1
#PBS -l walltime=00:60:00
#PBS -m e
#PBS -q workq
#PBS -N NGAS_client
#PBS -A cwu-icrar
#PBS -W group_list=astronomy556
#PBS -o /home/cwu/ngas_run/client_run.out
#PBS -e /home/cwu/ngas_run/client_run.err

source /home/cwu/dc_env.sh