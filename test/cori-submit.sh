#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 10
#SBATCH -t 00:30:00
#SBATCH -J fits-hdf-parallel
#SBATCH -e %j.err
#SBATCH -o %j.out

cd $SLURM_SUBMIT_DIR
python -W ignore h5fits-parallel.py 300 3000 4000
