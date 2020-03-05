# script runs automated tests on data conversions
import sys
import os
import ios_data_transform as iod
from glob import glob



def fix_path(path):
    # converts path from posix to nt if system is nt
    # input is string with path in posix format '/' file sep
    if os.name == 'nt':
        path = os.path.sep.join(path.split('/'))
    return path


def convert_mctd_files(f, out_path):
    fdata = iod.MCtdFile(filename=f, debug=False)
    if fdata.import_data():
        fdata.assign_geo_code(fix_path('test_files/ios_polygons.geojson'))
        iod.write_mctd_ncfile(fix_path(out_path+f.split(os.path.sep)[-1]+'.nc'), fdata)
        iod.add_standard_variables(fix_path(out_path + f.split(os.path.sep)[-1] + '.nc'))
    else:
        print("Unable to import data from file", fdata.filename)


def convert_bot_files(f, out_path):
    fdata = iod.BotFile(filename=f, debug=False)
    print(fdata.filename)
    if fdata.import_data():
        # print(fdata.data)
        fdata.assign_geo_code(fix_path('test_files/ios_polygons.geojson'))
        iod.write_ctd_ncfile(fix_path(out_path+f.split(os.path.sep)[-1]+'.nc'), fdata)
        iod.add_standard_variables(fix_path(out_path + f.split(os.path.sep)[-1] + '.nc'))

    else:
        print("Unable to import data from file", fdata.filename)


def convert_ctd_files(f, out_path):
    fdata = iod.CtdFile(filename=f, debug=False)
    print(fdata.filename)
    if fdata.import_data():
        # print(fdata.data)
        fdata.assign_geo_code(fix_path('test_files/ios_polygons.geojson'))
        iod.write_ctd_ncfile(fix_path(out_path+f.split(os.path.sep)[-1]+'.nc'), fdata)
        iod.add_standard_variables(fix_path(out_path+f.split(os.path.sep)[-1]+'.nc'))
    else:
        print("Unable to import data from file", fdata.filename)


for fn in glob(fix_path('./test_files/ctd_mooring/*.*'), recursive=True): 
    convert_mctd_files(f=fn, out_path=fix_path('temp/'))



for fn in glob(fix_path('./test_files/ctd_profile/*.*'), recursive=True):
    convert_ctd_files(f=fn, out_path=fix_path('temp/'))

for fn in glob(fix_path('./test_files/bot/*.*'), recursive=True):
    convert_bot_files(f=fn, out_path=fix_path('temp/'))

# print(iod.utils.compare_file_list(['a.bot', 'c.bkas.asd'], ['nc2/a.nc', 'nc3/nc1/b.nc', 'nc1/nc/c.nc', 'd.nc']))
