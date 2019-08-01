from ObsFile import CtdFile
from write_ctd_ncfile_cls import write_ctd_ncfile
# import ios_data_transform as iod
import glob
import os
from utils import import_env_variables, file_mod_time

def convert_ctd_files(in_path, out_path):
    flist = glob.glob(in_path+'*.[Cc][Tt][Dd]')
    print("Total number of files =", len(flist))
    # loop through files in list, read the data and write netcdf file if data read is successful
    for i, f in enumerate(flist[:]):
        # skip processing file if its newer than 24 hours old
        if file_mod_time(f) > 24.:
            continue
        else:
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


env_var = import_env_variables('./.env')
print(env_var)
convert_ctd_files(in_path=env_var['ctd_raw_folder'], out_path=env_var['ctd_nc_folder'])
# convert_ctd_files(path='test_files/')
# f = ObsFile.CtdFile(filename='/home/pramod/data/ios_mooring_data/ctd/1997-11-0131.CTD', debug=True)
# f = CurFile(filename='A1_19921028_19930504_0035m.CUR')
