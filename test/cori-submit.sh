#!/bin/bash
#SBATCH -p regular
#SBATCH -N 13
#SBATCH -t 00:50:00
#SBATCH -J celestial-convert
#SBATCH -e %j.err
#SBATCH -o %j.out
#SBATCH --mail-user=jalnliu@lbl.gov
#SBATCH --mail-type=ALL
cd $SLURM_SUBMIT_DIR
python -W ignore h5celestial-convert.py 300 0 90000 /project/projectdirs/cosmo/data/sdss/dr12/boss/photoObj/301/  /scratch1/scratchdirs/jialin/celestial/dr12_full2
