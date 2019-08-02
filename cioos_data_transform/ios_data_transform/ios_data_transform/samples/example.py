import sys
import os
sys.path.insert(0, os.getcwd()+'/../../')
import ios_data_transform as iod
import glob



def convert_ctd_files(in_path, out_path):
    flist = glob.glob(in_path+'*.[Cc][Tt][Dd]')
    print("Total number of files =", len(flist))
    # loop through files in list, read the data and write netcdf file if data read is successful
    for i, f in enumerate(flist[:]):
        print(i, f)
        fdata = iod.CtdFile(filename=f, debug=False)
        if fdata.import_data():
            print(" <- Read data successfully ! ")
            yy = fdata.date[0:4]
            if not os.path.exists(out_path+yy):
                os.mkdir(out_path+yy)
            iod.write_ctd_ncfile(out_path+yy+'/'+f.split('/')[-1][0:-4]+'.ctd.nc', fdata)
        else:
            print(" <- Failed to read file")


env_var = iod.import_env_variables('./.env')
print(env_var)
convert_ctd_files(in_path=env_var['ctd_raw_folder'], out_path=env_var['ctd_nc_folder'])