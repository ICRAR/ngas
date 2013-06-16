#!/bin/bash
# essential path and variables
#load modules needed for MWA_Tools

PATH=/scratch/astronomy556/MWA/RTS/bin:${PATH}

module unload intel
module load gcc
module load python numpy scipy
module load matplotlib
module load intel-mkl/10.3.9

module load pgplot
module load wcslib
module load cfitsio
module load openmpi
module load fftw/3.3.3
module load cuda

export LD_LIBRARY_PATH=/scratch/astronomy556/MWA/lib/:${LD_LIBRARY_PATH}
export RTS_DIR=/scratch/astronomy556/MWA/RTS/
export MWA_DIR=/scratch/astronomy556/MWA/

group=astronomy556