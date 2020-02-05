# script runs automated tests on data conversions
import sys
import os
sys.path.insert(0, os.getcwd()+'/../../')
import ios_data_transform as iod


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

convert_mctd_files(f='./test_files/ctd_mooring/a1_20140627_20150801_0100m.ctd', out_path='./temp/')

convert_ctd_files(f='./test_files/ctd_profile/2017-42-1021.ctd', out_path='./temp/')
# convert_bot_files(f='./test_files/bot/1950-001-0044.bot', out_path='./temp/')
