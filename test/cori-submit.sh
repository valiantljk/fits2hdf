#!/bin/bash
#SBATCH -p debug 
#SBATCH -N 1
#SBATCH -t 00:30:00
#SBATCH -J fits2hdf-parallel
#SBATCH -e %j.err
#SBATCH -o %j.out

cd $SLURM_SUBMIT_DIR
python -W ignore h5fits-parallel.py 20 3500 3600 /global/projecta/projectdirs/sdss/data/sdss/dr12/boss/spectro/redux/v5_7_0/ /global/cscratch1/sd/jialin/hdf-data/v5_7_0/
