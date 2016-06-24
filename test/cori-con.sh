#!/bin/bash
#SBATCH -p regular
#SBATCH -N 42
#SBATCH -t 00:50:00
#SBATCH -J concatenate-parallel
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH --mail-user=jalnliu@lbl.gov
#SBATCH --mail-type=ALL

cd $SLURM_SUBMIT_DIR
hdf5path="/scratch1/scratchdirs/jialin/celestial/dr12_full"
output="/scratch1/scratchdirs/jialin/celestial/concatenated/uerr.h5"
dataset="HDU1/DATA/UERR"

srun -n 1008 python-mpi -W ignore h5concatenate.py 0 79500 $hdf5path $output $dataset
