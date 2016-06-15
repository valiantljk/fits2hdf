import os
import sys
sys.path.append('..')
import time
from fits2hdf.io.fitsio import *
from fits2hdf.io.hdfio import *
from fits2hdf import idi
import numpy as np
from astropy.io import fits as pf
import h5py
rootdir="/global/projecta/projectdirs/sdss/data/sdss/dr12/boss/spectro/redux/v5_7_0/"
outputdir ="/global/cscratch1/sd/jialin/hdf-data/v5_7_0/"
def listfiles(x):
     fitsfiles = [os.path.join(root, name)
       for root, dirs, files in os.walk(x)
       for name in files
       if name.endswith((".fits", ".fits.gz"))]
     return fitsfiles

def test_multihdf(x):
     thedir = rootdir+str(x)+"/"
     try:
         count=0
         fitlist=listfiles(thedir)
	 for fname in fitlist:
             a = read_fits(fname) 
	     commonpath=os.path.commonprefix([fname,thedir])
	     subgroup=fname[len(commonpath):len(fname)]
             outputf=outputdir+str(x)+".h5"
	     export_hdf(a,outputf, root_group=subgroup)
             count=count+1
     except TypeError:
         print x
     finally:
         pass
def h5fit_test1():
     if (len(sys.argv)!=4):
       print "usage: python -W ignore h5fits-serial.py rootdir input_folder_name outputdir"
       print "example: python h5fits-serial.py /global/projecta/projectdirs/sdss/data/sdss/dr12/boss/spectro/redux/v5_7_0/ 4440 /global/cscratch1/sd/jialin/hdf-data/v5_7_0/"
       print "*****************End********************"
       sys.exit(1)
     global rootdir
     global outputdir
     rootdir=sys.argv[1]
     input=sys.argv[2]
     outputdir=sys.argv[3]
     thedir = rootdir+str(input)+"/"
     numfits = listfiles(thedir)
     print "*****************Start******************"
     print "Totally,",len(numfits),"fits files in folder:", thedir
     if (len(numfits)==0):
        print "No fits exists, Exit program"
        print "*****************End********************"
        sys.exit(1)
     print "****************In Progress*************"
     start = time.time()
     test_multihdf(input)
     end = time.time()
     print "Combined",len(numfits),"fits to hdf5, costs ", end-start, " seconds"
     print "Check output at ", outputd
     print "*****************End********************"
if __name__ == '__main__':
    h5fit_test1()
