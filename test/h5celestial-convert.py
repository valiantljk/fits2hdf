import os
import sys
sys.path.append('..')

from fits2hdf.io.fitsio import *
from fits2hdf.io.hdfio import *
from fits2hdf import idi
import numpy as np
from astropy.io import fits as pf
import h5py
from multiprocessing import Pool
import time
totalist=[]
rootdir="/project/projectdirs/cosmo/data/sdss/dr12/boss/photoObj/301/"
outputdir="/scratch1/scratchdirs/jialin/celestial/dr12"
#function for listing all fits files and fits.gz files inside given path and all sub-folders
def listfiles(x):
     print rootdir+str(x)+'/'+str(1) 
     fitsfiles = [os.path.join(root, name)
       for inx in range(1,7)
       for root, dirs, files in os.walk(rootdir+str(x)+'/'+str(inx))
       for name in files
       if name.endswith((".fits", ".fits.gz"))]
     global totalist
     totalist=totalist+fitsfiles
#function for combining one fits folder into a single HDF5 file
def test_multihdf(x):
    assert os.path.isfile(x)
    fname=x
    try: 
        a = read_fits(fname) 
        import ntpath
        ffname=str(ntpath.basename(fname))
        outputf=outputdir+ffname.split('.')[0]+".h5"
        export_hdf(a, outputf)
    except Exception, e:
        print "ioerror:%s"%e, fname
    finally:
        pass
def parallel_test_multihdf():
     if (len(sys.argv)!=6):
       print "usage: python -W ignore h5fits-parallel.py number_of_processes start end rootdir outputdir"
       print "example: python h5fits-parallel.py 32 1000 2000"
       print "*****************End********************"
       sys.exit(1)
     n=int(sys.argv[1])
     down=int(sys.argv[2])
     up=int(sys.argv[3])
     global rootdir
     global outputdir
     rootdir=sys.argv[4]
     outputdir=sys.argv[5]
     if(not rootdir.endswith("/")):
	rootdir=rootdir+"/"
     if(not outputdir.endswith("/")):
	outputdir=outputdir+"/"
     ldir=os.listdir(rootdir)    
     lldir=[fn for fn in ldir if fn.isdigit()]
     print len(lldir)
     lldir=map(int,lldir)
     lldir.sort()
     if up>len(lldir): 
	up=len(lldir)
     if down<0:
        down=0
     if down>len(lldir):
	down=0
     if down>up:
        temp=up
	up=down
	down=temp
     lldir=lldir[down:up]#the range to be processed
     global totalist
     map(listfiles,lldir)
   
     lldir=totalist
     print totalist[0]
     print "*****************Start******************"
     print "Totally, ", len(lldir), "fits folders,e.g., 4440 in root folder: ", rootdir
     if (len(lldir)==0):
        print "No fits folder exists, Exit program"
        print "*****************End********************"
        sys.exit(1)
     print "****************In Progress*************"
     start = time.time()
     if n > len(lldir):
        n=len(lldir)
     print "used ", n, "processes"
     p=Pool(n)
     p.map(test_multihdf,lldir)
     end = time.time()
     print "Combined",len(lldir),"fits folder(",down,"-",up,") to hdf5 files, costs ", end-start, " seconds"
     print "Check output at ", outputdir
     print "*****************End********************"
if __name__ == '__main__':
    parallel_test_multihdf()
