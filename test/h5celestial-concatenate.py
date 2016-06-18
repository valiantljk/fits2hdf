import os
import sys
sys.path.append('..')
import numpy as np
import h5py
import time
from mpi4py import MPI
rootdir="/scratch1/scratchdirs/jialin/celestial/dr12"
totalist=[]
output="/scratch1/scratchdirs/jialin/celestial/dr12_allinone"
conc_dtype="f8"
conc_dims=[]
check=0
#function for listing all fits files and fits.gz files inside given path and all sub-folders

def listfiles(x):
     fitsfiles = [os.path.join(root, name)
       for root, dirs, files in os.walk(x)
       for name in files
       if name.endswith((".h5", ".hdf5"))]
     return fitsfiles
def concat_range(range_files,dataset):
     #find the same dataset in all the files and then concatenate them into a single chunk
    data = np.array([])
    count = 0
    if(len(range_files)==0):
     return (data, count)
    try: 
     fx = h5py.File(range_files[0],'r')
     dx = fx[dataset]
     data = dx.value
     count=len(data)
     global check
     if (check==0):
	global conc_dims
        global conc_dtype
	conc_dims = dx.shape
	conc_dtype = dx.dtype 
	check=1
     fx.close()
    except Exception, e:
     print range_files[0]
    finally:
     pass 
    for i in range(1,len(range_files)):
	try: 
	 fx=h5py.File(range_files[i],'r')
	 data1=fx[dataset].value
	 count+=len(data1)
         data=np.concatenate((data,data1),axis=0)
	 fx.close()
	except Exception,e:
	 print "concatenation error %s"%range_files[i]
	finally:
	 pass
    return (data,count)
def parallel_concate():
     if (len(sys.argv)!=7):
       print "usage: python -W ignore h5fits-parallel.py number_of_processes start end rootdir output dataset"
       print "example: python h5fits-parallel.py 32 1000 2000"
       print "*****************End********************"
       sys.exit(1)
     n=int(sys.argv[1])
     down=int(sys.argv[2])
     up=int(sys.argv[3])
     global rootdir
     global output
     rootdir=sys.argv[4]
     output=sys.argv[5]
     dataset=sys.argv[6]
     if(not rootdir.endswith("/")):
	rootdir=rootdir+"/"
     lldir=listfiles(rootdir)
     up = (up, len(lldir))[up<len(lldir) and up>0]
     down = (0,down)[ down<0 or down>len(lldir)]
     if down>up:
       up,down=down,up
     lldir=lldir[down:up]#the range to be processed
     start = time.time()
     #setup mpi env
     comm =MPI.COMM_WORLD
     nproc = comm.Get_size()
     rank = comm.Get_rank()
     total_files=len(lldir)
     print total_files
     #distribute the workload evenly to each process
     step=total_files / nproc
     rank_start = rank * step
     rank_end = rank_start+step
     if(rank==nproc-1):
	rank_end=total_files-1
     range_files=lldir[rank_start:rank_end]
     #process files to concatenate the same dataset. This is done in parallel, i.e., each process handles its own range of files
     # conc_data contains one concatenated dataset from the files in current range. 
     # conc_len is the dimension size of such concatenated dataset
     (conc_data,conc_len)=concat_range(range_files,dataset)

     #compute the length of the final concatenated file
     final_len=np.zeros(0)
     try:
        comm.Reduce(conc_len, final_len,op=MPI.SUM,root=0)
     except Exception,e:
	print "computing final len returns error"
     #gather each local concatenated length into an array hosted by root

     comm.Barrier()
     if rank == 0:
      gather_range = np.zeros(nproc)
     else: 
      gather_range = None
     try:
      comm.Gatherv (conc_len, [gather_range,tuple([1]*nproc),tuple(range(0,nproc)),MPI.INT],root=0)
     except Exception,e:
      print "gather error, rank %d"%rank
     #generate an ascending offset list within rank 0, then broadcast the corresponding offset value to each process
     if rank==0:
	if len(gather_range)==0:
	   print "gathered range is empty"
	temp = 0
	for i in range(1,len(gather_range)):
	   temp1=gather_range[i]
	   gather_range[i]=gather_range[i-1]+temp 
	   temp=temp1
     #broad gather_range[i] to rank i
     try:
	comm.Scatterv([gather_range,tuple([1]*nproc),tuple(range(0,nproc)),MPI.INT],conc_len,root=0)   
     except Exception,e:
	print "scatter error"

     # create the final hdf5 file
     try:
      conc_f=h5py.File(output,'w',driver='mpio',comm=MPI.COMM_WORLD) 
      #need a sanity check to determine the data type, dimension info.
      global conc_dtype
      global conc_dims
      conc_dims_array = []
      for item in conc_dims:
       conc_dims_array.extend(item)
      conc_dims_array[0]=final_len	
      conc_d = f.create_dataset(dataset, conc_dims_array, dtype=conc_dtype)
     except Exception,e:
      print "final file creation error"
     #all processes write to the designated location by filling the pre-created hdf5 file in certain range
     try:
      conc_d[conc_len,:]=conc_data
     except Exception,e:
      print "parallel write error"
     comm.Barrier()
     conc_f.close()
     end = time.time()
     if (rank==0):
      print "Combined",len(lldir),"fits folder(",down,"-",up,") to hdf5 files, costs ", end-start, " seconds"
      print "Check output at ", output
      print "*****************End********************"
if __name__ == '__main__':
    parallel_concate()
