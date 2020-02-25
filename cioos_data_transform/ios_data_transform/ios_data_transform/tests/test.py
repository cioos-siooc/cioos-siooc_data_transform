# script runs automated tests on data conversions
import sys
import os
sys.path.insert(0, os.getcwd()+'/../../')
import ios_data_transform as iod
from glob import glob

def convert_mctd_files(f, out_path):
    fdata = iod.MCtdFile(filename=f, debug=False)
    if fdata.import_data():
        fdata.assign_geo_code('../tests/test_files/ios_polygons.geojson')
        iod.write_mctd_ncfile(out_path+f.split('/')[-1]+'.nc', fdata)
    else:
        print("Unable to import data from file", fdata.filename)


def convert_bot_files(f, out_path):
    fdata = iod.BotFile(filename=f, debug=False)
    print(fdata.filename)
    if fdata.import_data():
        # print(fdata.data)
        fdata.assign_geo_code('../tests/test_files/ios_polygons.geojson')
        iod.write_ctd_ncfile(out_path+f.split('/')[-1]+'.nc', fdata)
    else:
        print("Unable to import data from file", fdata.filename)


def convert_ctd_files(f, out_path):
    fdata = iod.CtdFile(filename=f, debug=False)
    print(fdata.filename)
    if fdata.import_data():
        # print(fdata.data)
        fdata.assign_geo_code('../tests/test_files/ios_polygons.geojson')
        iod.write_ctd_ncfile(out_path+f.split('/')[-1]+'.nc', fdata)
    else:
        print("Unable to import data from file", fdata.filename)


for fn in glob('./test_files/ctd_mooring/*.*', recursive=True):
    convert_mctd_files(f=fn, out_path='./temp/')

for fn in glob('./test_files/ctd_profile/*.*', recursive=True):
    convert_ctd_files(f=fn, out_path='./temp/')

for fn in glob('./test_files/bot/*.*', recursive=True):
    convert_bot_files(f=fn, out_path='./temp/')

# print(iod.utils.compare_file_list(['a.bot', 'c.bkas.asd'], ['a.nc', 'b.nc', 'c.nc', 'd.nc']))
