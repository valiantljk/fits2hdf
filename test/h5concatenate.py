# HDF5 parallel concatenation code
# This code concatenates the same dataset from all input hdf5 files, in parallel
# Input: 
# Output: 
# Jialin Liu
# Jun 19 2016
# jalnliu@lbl.gov

import os
import sys
sys.path.append('..')
import numpy as np
import h5py
import time
from mpi4py import MPI
import traceback
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
     #print data
     count=len(data)
     #print "len is %d for file %s"%(len(data),range_files[0])
     #print "size of data in bytes %d "%data.nbytes
     global check
     if (check==0):
	global conc_dims
        global conc_dtype
	conc_dims = dx.shape
	conc_dtype = dx.dtype 
	check=1
     fx.close()
    except Exception, e:
     print "first file read error:%s"%range_files[0]
    finally:
     pass 
    lrange=len(range_files)
    #print "len of range: %d"%lrange
    for i in range(1,lrange):
	try: 
	 fx=h5py.File(range_files[i],'r')
	 data1=fx[dataset].value
	 count+=len(data1)
	 #print "len is %d for file %s"%(len(data1),range_files[i])
         data=np.concatenate((data,data1),axis=0)
	 #print "size of data in bytes %d "%data.nbytes
	 fx.close()
	except Exception,e:
	 print "concatenation error %s"%range_files[i]
         #traceback.print_exc()
	finally:
	 pass
    return (data,count)
def parallel_concate():
     if (len(sys.argv)!=6):
       print "usage: srun -n 10 python-mpi -W ignore h5concatenate.py start end rootdir output.h5 dataset"
       print "example: python-mpi h5concatenate.py 0 1000 path_hdf5 output.h5 /HDU1/DATA/UERR"
       print "*****************End********************"
       sys.exit(1)
     #n=int(sys.argv[1])
     down=int(sys.argv[1])
     up=int(sys.argv[2])
     global rootdir
     global output
     rootdir=sys.argv[3]
     output=sys.argv[4]
     dataset=sys.argv[5]
     if(not rootdir.endswith("/")):
	rootdir=rootdir+"/"
     lldir=listfiles(rootdir)
     up = (len(lldir),up)[up<len(lldir) and up>0]
     down = (down,0)[ down<0 or down>len(lldir)]
     if down>up:
       up,down=down,up
     lldir=lldir[down:up]#the range to be processed
     start = time.time()
     #setup mpi env
     comm =MPI.COMM_WORLD
     nproc = comm.Get_size()
     rank = comm.Get_rank()
     total_files=len(lldir)
     if rank==0:
      print "Total files: %d"%total_files
     #distribute the workload evenly to each process
     step=total_files / nproc
     rank_start = rank * step
     rank_end = rank_start+step
     if(rank==nproc-1):
	rank_end=total_files
     range_files=lldir[rank_start:rank_end]
     #process files to concatenate the same dataset. This is done in parallel, i.e., each process handles its own range of files
     # conc_data contains one concatenated dataset from the files in current range. 
     # conc_len is the dimension size of such concatenated dataset
     (conc_data,conc_len)=concat_range(range_files,dataset)
     conc_data = np.array(conc_data)
     if rank==0:
      print "Number of processes: %d"%nproc
     conc_len=np.array(conc_len)
     #print "local len is %d for rank %d"%(conc_len,rank)
     #compute the length of the final concatenated file
     final_len=np.array(0)
     try:
        comm.Reduce(np.array(conc_len), final_len,op=MPI.SUM,root=0)
     except Exception,e:
	print "computing final len returns error"
	traceback.print_exc()
     #broadcast this global length to all processes, in order to create dataset collectively
     try: 
        final_len=comm.bcast(final_len,root=0)
     except Exception,e:
        print "broadcast final len error"
     #gather each local concatenated length into an array hosted by root
     #if rank==0:
     # print "final_len %d"%final_len
     comm.Barrier()
     if rank == 0:
      gather_range = np.zeros(nproc,dtype=np.int)
     else: 
      gather_range = None
     try:
      comm.Gatherv (conc_len, [gather_range,tuple([1]*nproc),tuple(range(0,nproc)),MPI.LONG],root=0)
     except Exception,e:
      print "gather error, rank %d"%rank
      traceback.print_exc()
     if rank==0:
      print "Length of the dataset in each file: ",gather_range
     #generate an ascending offset list within rank 0, then broadcast the corresponding offset value to each process
     if rank==0:
	if len(gather_range)==0:
	   print "gathered range is empty"
           sys.exit(0)
        temp=gather_range[0]
	gather_range[0]=0
	for i in range(1,len(gather_range)):
           temp1=gather_range[i]
	   gather_range[i]=gather_range[i-1]+temp
	   temp=temp1
     #broad gather_range[i] to rank i
     #check in rank 0
     conc_start=np.zeros(1)
     #if rank==0:
     # for i in range(0,len(gather_range)):
     #  print "before scatter, gather_range[%d]=%d"%(i,gather_range[i])
     try:
	#comm.Scatterv([gather_range,tuple([1]*nproc),tuple(range(0,nproc)),MPI.INT],conc_start,root=0)   
        conc_start=comm.scatter(gather_range,root=0)
     except Exception,e:
	print "scatter error"
     comm.Barrier()
     #print "after scatter, rank %d start at %d length %d"%(rank,conc_start,conc_len)
     # create the final hdf5 file
     try:
      conc_f=h5py.File(output,'w',driver='mpio',comm=MPI.COMM_WORLD) 
      #need a sanity check to determine the data type, dimension info.
      global conc_dtype
      global conc_dims
      conc_dims_array = list()
      for item in conc_dims:
        conc_dims_array.append(item)
      conc_dims_array=np.array(conc_dims_array)
      conc_dims_array[0]=final_len
      if rank==0:
       print "Concatenated dataset shape ",conc_dims_array
      conc_d = conc_f.create_dataset(dataset, conc_dims_array, dtype=conc_dtype)
     except Exception,e:
      print "final dataset creation error"
      traceback.print_exc()
     if rank==0:
       print "Concatenated dataset object ",conc_d       
     #all processes write to the designated location by filling the pre-created hdf5 file in certain range
     #print conc_d, "rank %d"%rank
     try:
      #print "start %d, end %d"%(conc_start,conc_len)
      #print "local data shape ",len(conc_data)
      #print "local data type: ",conc_data.dtype
      #print "hdf5 data set: ",conc_d
      #with conc_d.collective:
      conc_d[int(conc_start):int(conc_start+conc_len),:]=conc_data
     except Exception,e:
      print "parallel write error"
      traceback.print_exc()
     comm.Barrier()
     conc_f.close()
     end = time.time()
     if (rank==0):
      print "Combined %d hdf5 files (%d-%d), costs %.3f seconds"%(len(lldir),down,up,end-start)
      print "Check output: %s "%output
      print "*****************End********************"
if __name__ == '__main__':
    parallel_concate()
