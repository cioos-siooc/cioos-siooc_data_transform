from ObsFile import CtdFile
from write_ctd_ncfile_cls import write_ctd_ncfile
# import ios_data_transform as iod
import glob
import os

def convert_ctd_files(path=None):
    # find all ctd files in the path provided
    if path is None:
        flist = glob.glob('/home/pramod/data/ios_mooring_data/ctd/*.ctd')
    else:
        flist = glob.glob(path+'/*.ctd')
    print("Total number of files =", len(flist))
    out_path = '/home/pramod/temp/'
    # loop through files in list, read the data and write netcdf file if data read is successful
    for i, f in enumerate(flist[:2]):
        print(i, f)
        fdata = CtdFile(filename=f, debug=False)
        if fdata.import_data():
            # print " <- Read data successfully !"
            yy = fdata.date[0:4]
            if not os.path.exists(out_path+yy):
                os.mkdir(out_path+yy)
            write_ctd_ncfile(out_path+yy+'/'+f.split('/')[-1][0:-4]+'.ctd.nc', fdata)
        else:
            print(" <- Failed to read file")


convert_ctd_files(path=None)
# convert_ctd_files(path='test_files/')
# f = ObsFile.CtdFile(filename='/home/pramod/data/ios_mooring_data/ctd/1997-11-0131.CTD', debug=True)
# f = CurFile(filename='A1_19921028_19930504_0035m.CUR')
