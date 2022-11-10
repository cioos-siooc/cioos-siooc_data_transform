# script runs automated tests on data conversions
import os
import cioos_data_transform.IosObsFile as ios
import cioos_data_transform.utils as cioos_utils
from ..write_ctd_ncfile import write_ctd_ncfile
from ..write_cur_ncfile import write_cur_ncfile
from ..write_mctd_ncfile import write_mctd_ncfile
from cioos_data_transform.utils import fix_path
from glob import glob
import unittest

GEOJSON_AREAS_PATH = fix_path(
    "./projects/ios_data_transform/tests/test_files/ios_polygons.geojson"
)
OUTPUT_PATH = fix_path("./projects/ios_data_transform/tests/temp/")


def convert_mctd_files(f, out_path):
    fdata = ios.MCtdFile(filename=f, debug=False)
    if fdata.import_data():
        fdata.assign_geo_code(GEOJSON_AREAS_PATH)
        write_mctd_ncfile(fix_path(out_path + f.split(os.path.sep)[-1] + ".nc"), fdata)
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
        fdata.assign_geo_code(GEOJSON_AREAS_PATH)
        write_ctd_ncfile(fix_path(out_path + f.split(os.path.sep)[-1] + ".nc"), fdata)
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
        fdata.assign_geo_code(GEOJSON_AREAS_PATH)
        write_ctd_ncfile(fix_path(out_path + f.split(os.path.sep)[-1] + ".nc"), fdata)
        cioos_utils.add_standard_variables(
            fix_path(out_path + f.split(os.path.sep)[-1] + ".nc")
        )
    else:
        print("Unable to import data from file", fdata.filename)


def convert_cur_files(f, out_path):
    print(f)
    fdata = ios.CurFile(filename=f, debug=False)
    if fdata.import_data():
        fdata.assign_geo_code(GEOJSON_AREAS_PATH)
        write_cur_ncfile(fix_path(out_path + f.split(os.path.sep)[-1] + ".nc"), fdata)
        # iod.utils.add_standard_variables(fix_path(out_path + f.split(os.path.sep)[-1] + '.nc')) #Ignore for now
    else:
        print("Unable to import data from file", fdata.filename)


def convert_any_files(f, out_path):
    print(f)
    fdata = ios.GenFile(filename=f, debug=False)

    if fdata.import_data():
        fdata.assign_geo_code(GEOJSON_AREAS_PATH)
        fdata.add_ios_vocabulary()
        ds = fdata.to_xarray()

        ds.to_netcdf(out_path + f.split(os.path.sep)[-1] + ".nc")


class TestIOSConversion(unittest.TestCase):
    """Test IOS Files Conversion with different methods"""

    def test_mctd_files(self):
        """Test to parse mctd files with the original method conversion"""
        files = glob(
            fix_path("./projects/ios_data_transform/tests/test_files/ctd_mooring/*.*"),
            recursive=True,
        )
        assert len(files) > 0, "No files found for conversion tests"
        for fn in files:
            convert_mctd_files(f=fn, out_path=OUTPUT_PATH)

    def test_cur_files(self):
        """Test to parse cur files with the original method conversion"""
        files = glob(
            fix_path(
                "./projects/ios_data_transform/tests/test_files/current_meter/*.*"
            ),
            recursive=True,
        )
        assert len(files) > 0, "No files found for conversion tests"
        for fn in files:
            convert_cur_files(f=fn, out_path=OUTPUT_PATH)

    def test_bot_files(self):
        """Test to parse bot files with the original method conversion"""
        files = glob(
            fix_path("./projects/ios_data_transform/tests/test_files/bot/*.*"),
            recursive=True,
        )
        assert len(files) > 0, "No files found for conversion tests"
        for fn in files:
            convert_bot_files(f=fn, out_path=OUTPUT_PATH)

    def test_all_ios_files(self):
        """Test to convert all the file types by using the general parser"""
        files = glob(
            fix_path("./projects/ios_data_transform/tests/test_files/**/*.*"),
            recursive=True,
        )
        assert len(files) > 0, "No files found for conversion tests"
        for fn in files:
            if fn.endswith(("geojson")):
                continue
            convert_any_files(f=fn, out_path=OUTPUT_PATH)

    # TODO compare general parser generated netcdfs vs original netcdfs
