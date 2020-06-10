import os
import sys
import glob
from multiprocessing import Process
from time import time
import ios_data_transform as iod
import subprocess


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
        fgeo = env_vars['geojson_file']
        flist = glob.glob(in_path + '**/*.[Cc][Tt][Dd]', recursive=True)
    elif ftype == 'mctd':
        in_path = env_vars['mctd_raw_folder']
        out_path = env_vars['mctd_nc_folder']
        fgeo = env_vars['geojson_file']
        flist = []
        flist.extend(glob.glob(in_path + '**/*.[Cc][Tt][Dd]', recursive=True))
        flist.extend(glob.glob(in_path + '**/*.mctd', recursive=True))
    elif ftype == 'bot':
        in_path = env_vars['bot_raw_folder']
        out_path = env_vars['bot_nc_folder']
        fgeo = env_vars['geojson_file']
        flist = []
        flist.extend(glob.glob(in_path + '**/*.[Bb][Oo][Tt]', recursive=True))
        flist.extend(glob.glob(in_path + '**/*.[Cc][Hh][Ee]', recursive=True))
    else:
        print("ERROR: Filetype not understood ...")
        return None
    print("Total number of files =", len(flist))
    # loop through files in list, read the data and write netcdf file if data read is successful
    for i, fname in enumerate(flist[:]):
        # print('\nProcessing -> {} {}'.format(i, fname))
        p = Process(target=(convert_files_threads), args=(ftype, fname, fgeo, out_path))
        p.start()
        p.join()
    return flist


def standardize_variable_names(ncfile):
    # input: netcdf file with non-standard variable names
    # output: netcdf file with standard variables added
    # NOTE: netcdf files are overwritten
    iod.add_standard_variables(ncfile)


def convert_files_threads(ftype, fname, fgeo, out_path):
    # skip processing file if its older than 24 hours old
    if iod.file_mod_time(fname) < -24. and opt == 'new':
        # print("Not converting file: ", fname)
        return 0
    print('Processing {} {}'.format(ftype, fname))
    # read file based on file type
    if ftype == 'ctd':
        fdata = iod.CtdFile(filename=fname, debug=False)
    elif ftype == 'mctd':
        fdata = iod.MCtdFile(filename=fname, debug=False)
    elif ftype == 'bot':
        fdata = iod.CtdFile(filename=fname, debug=False)
    else:
        print("Filetype not understood!")
        sys.exit()
    # if file class was created properly, try to import data
    if fdata.import_data():
        print("Imported data successfully!")
        fdata.assign_geo_code(fgeo)
        # now try to write the file...
        yy = fdata.start_date[0:4]
        if not os.path.exists(out_path + yy):
            os.mkdir(out_path + yy)
        ncFileName = out_path + yy + '/' + fname.split('/')[-1] + '.nc'
        if ftype == 'ctd':
            try:
                iod.write_ctd_ncfile(ncFileName, fdata)
                standardize_variable_names(ncFileName)
            except Exception as e:
                print("Error: Unable to create netcdf file:", fname, e)
                subprocess.call(['rm', '-f', ncFileName])
        elif ftype == 'mctd':
            try:
                iod.write_mctd_ncfile(ncFileName, fdata)
                standardize_variable_names(ncFileName)
            except Exception as e:
                print("Error: Unable to create netcdf file:", fname, e)
                subprocess.call(['rm', '-f', ncFileName])
        elif ftype == 'bot':
            try:
                iod.write_ctd_ncfile(ncFileName, fdata)
                standardize_variable_names(ncFileName)
            except Exception as e:
                print("Error: Unable to create netcdf file:", fname, e)
                subprocess.call(['rm', '-f', ncFileName])
    else:
        print("Error: Unable to import data from file", fname)
        return 0


# read inputs if any from the command line
# first input is 'all' or 'new' for processing all files or just files newer than 24 hours
# second input is file type and is one of ['ctd','mctd', 'cur', 'bot']
if len(sys.argv) > 1:
    opt = sys.argv[1].strip().lower()
    ftype = sys.argv[2].strip().lower()
else:  # default option. process all files !
    opt = 'all'
    ftype = 'ctd'
env_vars = iod.import_env_variables('./.env')
print('Inputs from .env file: ', env_vars)

start = time()
flist = convert_files(env_vars=env_vars, opt=opt, ftype=ftype)
print("Time taken to convert files: {:0.2f}".format(time() - start))
# if any raw files have been removed, delete corresponding netCDF files
if flist is not None:
    print("Checking if any netCDF files should be removed...")
    ncfilelist = glob.glob(env_vars[ftype + '_nc_folder'] + '**/*.nc', recursive=True)
    for i, e in enumerate(iod.utils.compare_file_list(sub_set=flist, global_set=ncfilelist)):
        filename = glob.glob(env_vars[ftype + '_nc_folder'] + '**/{}.nc')
        print('deleting file:', e)
        subprocess.call(['rm', '-f', e])
print("Total time taken:{:0.2f}".format(time() - start))
