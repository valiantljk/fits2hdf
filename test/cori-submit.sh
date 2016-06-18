#!/bin/bash
#SBATCH -p regular
#SBATCH -N 50
#SBATCH -t 00:20:00
#SBATCH -J fits2hdf-parallel
#SBATCH -e %j.err
#SBATCH -o %j.out

cd $SLURM_SUBMIT_DIR
python -W ignore h5celestial-convert.py 1200 0 1000000 /project/projectdirs/cosmo/data/sdss/dr12/boss/photoObj/301/  /scratch1/scratchdirs/jialin/celestial/dr12_full
