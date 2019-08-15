import sys
import os
sys.path.insert(0, os.getcwd()+'/../../')
import ios_data_transform as iod
import glob


def convert_files(env_vars, opt='all', ftype=None):
    # path of raw files, path for nc files, and option
    # opt = 'new' for only new raw files
    # opt = 'all' for all files. default value;
    # ftype =   'ctd' for CTD profiles
    #           'mctd' for mooring CTDs
    #           'cur' for currentmeters
    print('Option, ftype =', opt, ftype)
    if ftype == 'ctd':
        in_path = env_vars['ctd_raw_folder']
        out_path = env_vars['ctd_nc_folder']
        flist = glob.glob(in_path+'**.[Cc][Tt][Dd]')
    elif ftype == 'mctd':
        in_path = env_vars['mctd_raw_folder']
        out_path = env_vars['mctd_nc_folder']
        flist = glob.glob(in_path+'**.[Cc][Tt][Dd]')
    elif ftype in ['cur']:
        flist = glob.glob(in_path+'**.[Cc][Uu][Rr]')
    else:
        print("Filetype not understood ...")
        sys.exit()
    print("Total number of files =", len(flist))
    # loop through files in list, read the data and write netcdf file if data read is successful
    for i, fname in enumerate(flist[:]):
        # skip processing file if its older than 24 hours old
        if iod.file_mod_time(fname) < -24. and opt == 'new':
            continue
        print('\nProcessing -> {} {}'.format(i, fname))
        # read file based on file type
        if ftype == 'ctd':
            fdata = iod.CtdFile(filename=fname, debug=False)
        elif ftype == 'mctd':
            fdata = iod.MCtdFile(filename=fname, debug=False)
        else:
            print("Filetype not understood!")
            sys.exit()
    # if file class was created properly, try to import data
        if fdata.import_data():
            print("Imported data successfully!")
            # now try to write the file...
            yy = fdata.start_date[0:4]
            if not os.path.exists(out_path+yy):
                os.mkdir(out_path+yy)
            if ftype == 'ctd':
                iod.write_ctd_ncfile(out_path+yy+'/'+fname.split('/')[-1][0:-4]+'.ctd.nc', fdata)
            elif ftype == 'mctd':
                iod.write_mctd_ncfile(out_path+yy+'/'+fname.split('/')[-1][0:-4]+'.mctd.nc', fdata)
        else:
            print("failed to import data from file", fname)
            continue


# read inputs if any from the command line
# first input is 'all' or 'new' for processing all files or just files newer than 24 hours
# second input is file type and is one of ['ctd','mctd', 'cur', 'bot']
if len(sys.argv) > 1:
    opt = sys.argv[1].strip().lower()
    ftype = sys.argv[2].strip().lower()
else: # default option. process all files !
    opt = 'all'
env_vars = iod.import_env_variables('./.env')
print('Inputs from .env file: ', env_vars)

convert_files(env_vars=env_vars, opt=opt, ftype=ftype)
