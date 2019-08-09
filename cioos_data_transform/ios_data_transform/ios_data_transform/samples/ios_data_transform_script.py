import sys
import os
sys.path.insert(0, os.getcwd()+'/../../')
import ios_data_transform as iod
import glob


def convert_ctd_files(in_path, out_path, opt='all'):
    # path of raw files, path for nc files, and option
    # opt = 'new' for only new raw files
    # opt = 'all' for all files. default value; 
    #       'ctd' for only ctds
    #       'cur' for only currentmeters. etc ... 
    print('Options', opt)
    flist = glob.glob(in_path+'*.[Cc][Tt][Dd]')
    print("Total number of files =", len(flist))
    # loop through files in list, read the data and write netcdf file if data read is successful
    for i, f in enumerate(flist[:]):
        # skip processing file if its older than 24 hours old
        if iod.file_mod_time(f) < -24. and opt == 'new':
            continue
        print('\nProcessing -> {} {}'.format(i, f))
        try:
            fdata = iod.CtdFile(filename=f, debug=False)
            fdata.import_data()
            yy = fdata.start_date[0:4]
            if not os.path.exists(out_path+yy):
                os.mkdir(out_path+yy)
            iod.write_ctd_ncfile(out_path+yy+'/'+f.split('/')[-1][0:-4]+'.ctd.nc', fdata)
        except Exception as e:
            print(e)
            print(" <- Failed to read file")


# read inputs if any from the command line
if len(sys.argv) > 1:
    opt = sys.argv[1].strip().lower()
else: # default option. process all files !
    opt = 'all'
env_var = iod.import_env_variables('./.env')
print('Inputs from .env file: ', env_var)
convert_ctd_files(in_path=env_var['ctd_raw_folder'], out_path=env_var['ctd_nc_folder'], opt=opt)
