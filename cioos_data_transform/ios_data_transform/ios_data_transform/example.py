from ObsFile import CtdFile
from write_ctd_ncfile import write_ctd_ncfile
# import ios_data_transform as iod
import glob

def read_ctd_files(path=None):
    # find all ctd files in the path provided
    if path is None:
        flist = glob.glob('/home/pramod/data/ios_mooring_data/ctd/*.[cC][tT][dD]')
    else:
        flist = glob.glob(path+'/*.[cC][tT][dD]')
    print "Total number of files =", len(flist)
    # loop through files in list, read the data and write netcdf file if data read is successful
    for i, f in enumerate(flist[0:10]):
        print i, f,
        fdata = CtdFile(filename=f, debug=False)
        if fdata.import_data():
            print " <- Read data successfully !"
            write_ctd_ncfile('/home/pramod/temp/'+f.split('/')[-1]+'.nc', fdata)
        else:
            print " <- Failed to read file"


# read_ctd_files(path=None)
read_ctd_files(path='test_files/')
# f = ObsFile.CtdFile(filename='/home/pramod/data/ios_mooring_data/ctd/1997-11-0131.CTD', debug=True)
# f = CurFile(filename='A1_19921028_19930504_0035m.CUR')
