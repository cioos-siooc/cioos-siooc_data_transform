# script runs automated tests on data conversions
import os
import cioos_data_transform.IosObsFile as ios
import cioos_data_transform.utils as cioos_utils
from write_ctd_ncfile import write_ctd_ncfile
from write_cur_ncfile import write_cur_ncfile
from write_mctd_ncfile import write_mctd_ncfile
from cioos_data_transform.utils import fix_path
from glob import glob


def convert_mctd_files(f, out_path):
    fdata = ios.MCtdFile(filename=f, debug=False)
    if fdata.import_data():
        fdata.assign_geo_code(
            fix_path("tests/test_files/ios_polygons.geojson")
        )
        write_mctd_ncfile(
            fix_path(out_path + f.split(os.path.sep)[-1] + ".nc"), fdata
        )
        cioos_utils.add_standard_variables(
            fix_path(out_path + f.split(os.path.sep)[-1] + ".nc")
        )
    else:
        print("Unable to import data from file", fdata.filename)


def convert_bot_files(f, out_path):
    fdata = ios.BotFile(filename=f, debug=False)
    print(fdata.filename)
    if fdata.import_data():
        # print(fdata.data)
        fdata.assign_geo_code(
            fix_path("tests/test_files/ios_polygons.geojson")
        )
        write_ctd_ncfile(
            fix_path(out_path + f.split(os.path.sep)[-1] + ".nc"), fdata
        )
        cioos_utils.add_standard_variables(
            fix_path(out_path + f.split(os.path.sep)[-1] + ".nc")
        )

    else:
        print("Unable to import data from file", fdata.filename)


def convert_ctd_files(f, out_path):
    fdata = ios.CtdFile(filename=f, debug=False)
    print(fdata.filename)
    if fdata.import_data():
        # print(fdata.data)
        fdata.assign_geo_code(
            fix_path("tests/test_files/ios_polygons.geojson")
        )
        write_ctd_ncfile(
            fix_path(out_path + f.split(os.path.sep)[-1] + ".nc"), fdata
        )
        cioos_utils.add_standard_variables(
            fix_path(out_path + f.split(os.path.sep)[-1] + ".nc")
        )
    else:
        print("Unable to import data from file", fdata.filename)


def convert_cur_files(f, out_path):
    print(f)
    fdata = ios.CurFile(filename=f, debug=False)
    if fdata.import_data():
        fdata.assign_geo_code(
            fix_path("tests/test_files/ios_polygons.geojson")
        )
        write_cur_ncfile(
            fix_path(out_path + f.split(os.path.sep)[-1] + ".nc"), fdata
        )
        # iod.utils.add_standard_variables(fix_path(out_path + f.split(os.path.sep)[-1] + '.nc')) #Ignore for now
    else:
        print("Unable to import data from file", fdata.filename)


# for fn in glob(fix_path('./test_files/ctd_mooring/*.*'), recursive=True):
#     convert_mctd_files(f=fn, out_path=fix_path('temp/'))

# for fn in glob(fix_path('./test_files/ctd_profile/*.*'), recursive=True):
#     convert_ctd_files(f=fn, out_path=fix_path('temp/'))

# for fn in glob(fix_path('./test_files/bot/*.*'), recursive=True):
#     convert_bot_files(f=fn, out_path=fix_path('temp/'))

for fn in glob(
    fix_path("./tests/test_files/current_meter/*.*"), recursive=True
):
    convert_cur_files(f=fn, out_path=fix_path("./tests/temp/"))

# print(iod.utils.compare_file_list(['a.bot', 'c.bkas.asd'], ['nc2/a.nc', 'nc3/nc1/b.nc', 'nc1/nc/c.nc', 'd.nc']))
