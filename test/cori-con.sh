#!/bin/bash
#SBATCH -p debug
#SBATCH -N 2
#SBATCH -t 00:10:00
#SBATCH -J concatenate-parallel
#SBATCH -e %j.err
#SBATCH -o %j.out

cd $SLURM_SUBMIT_DIR
hdf5path="/scratch1/scratchdirs/jialin/celestial/dr12_test"
output="/scratch1/scratchdirs/jialin/celestial/concatenated/o5.h5"
dataset="HDU1/DATA/UERR"

srun -n 40 python-mpi -W ignore h5concatenate.py 0 3200 $hdf5path $output $dataset
