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
import csv
totalist=[]
rootdir="/project/projectdirs/cosmo/data/sdss/dr12/boss/photoObj/301/"
outputdir="/scratch1/scratchdirs/jialin/celestial/dr12"
#function for listing all fits files and fits.gz files inside given path and all sub-folders
prescanned="selected_files_938046.out"
def listfiles(x):
     #print rootdir+str(x)+'/'+str(1) 
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
     #ldir=os.listdir(rootdir)    
     #lldir=[fn for fn in ldir if fn.isdigit()]
     print "****************Start*******************"
     csv_tstart=time.time()
     print csv_tstart
     global totalist
     try:
	 with open(prescanned,'rb') as f:
	  reader = csv.reader(f)
	  totalist = list(reader)

     except Exception, e:
	 print ("input csv read error or not exist: %s"%e,prescanned)
     totalist = [x for sublist in totalist for x in sublist]
     print "csv reads costs %.2f seconds"%(time.time()-csv_tstart)
     #map(listfiles,lldir)
     #if(len(totalist)>0):
     # selected_f="selected_files_"+str(len(totalist))+".out"
     # print("Selected file info saved in %s"%str(selected_f))
     #with open(selected_f,"wb") as f:
     # f.writelines(["%s\n" % item  for item in totalist])     
     #print totalist[0] 
     lenfits=len(totalist)
     if down>up:
      (down,up)=(up,down)
     down = (0,down)[down>0] 
     up = (lenfits,up)[up<lenfits]  
     lldir=totalist[down:up]
     print "total number of fits:%d"%len(totalist)
     #print "*****************Start******************"
     print "Going to process %d fits files in %s"%(len(lldir),rootdir)
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
     print "Combined",len(lldir),"fits folder(",down,"-",up,") to hdf5 files, costs %.2f seconds "%(end-start)
     print "Check output at ", outputdir
     print "*****************End********************"
if __name__ == '__main__':
    parallel_test_multihdf()
