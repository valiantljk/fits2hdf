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
rootdir="/scratch1/scratchdirs/jialin/celestial/dr12"

outputdir="/scratch1/scratchdirs/jialin/celestial/dr12_allinone"
#function for listing all fits files and fits.gz files inside given path and all sub-folders
def listfiles(x):
     fitsfiles = [os.path.join(root, name)
       for root, dirs, files in os.walk(x)
       for name in files
       if name.endswith((".fits", ".fits.gz"))]
     return fitsfiles
#function for combining one fits folder into a single HDF5 file
def test_concact(x):
     print "input dir: ",x
     thedir = rootdir+str(x)+"/"
     try:
         count=0
         #fitlist=listfiles(thedir)
         print "number of files %d" % len(fitlist), "in ", thedir
         for fname in fitlist:
             try: 
                a = read_fits(fname) 
		#commonpath=os.path.commonprefix([fname,thedir])
		#subgroup=fname[len(commonpath):len(fname)]
		import ntpath
		ffname=str(ntpath.basename(fname))
		outputf=outputdir+ffname+".h5"
                export_hdf(a, outputf)
                count=count+1
             except Exception, e:
		print "ioerror:%s"%e, fname
             finally:
		pass
     except TypeError:
         print x
     finally:
         pass
def myfilter(x):
    if down < x and x < up:
      return x
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
     lldir=map(int,lldir)
     lldir.sort()
     lldir=lldir[down:up]
     #lldir=list([x for x in lldir if x>down and x<up])
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
     p.map(test_concact,lldir)
     end = time.time()
     print "Combined",len(lldir),"fits folder(",down,"-",up,") to hdf5 files, costs ", end-start, " seconds"
     print "Check output at ", outputd
     print "*****************End********************"
if __name__ == '__main__':
    parallel_test_multihdf()
