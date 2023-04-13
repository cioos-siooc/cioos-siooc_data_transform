# script runs automated tests on data conversions
import os
import unittest
import pytest
from glob import glob

import cioos_data_transform.IosObsFile as ios
import cioos_data_transform.utils as cioos_utils
from cioos_data_transform.utils import fix_path

from ios_data_transform import ios_data_transform_script
from ios_data_transform.write_ctd_ncfile import write_ctd_ncfile
from ios_data_transform.write_cur_ncfile import write_cur_ncfile
from ios_data_transform.write_mctd_ncfile import write_mctd_ncfile

MODULE_PATH = os.path.dirname(__file__)
GEOJSON_AREAS_PATH = fix_path(f"{MODULE_PATH}/test_files/ios_polygons.geojson")
GEOGRAPHICAL_AREAS = ios.read_geojson(GEOJSON_AREAS_PATH)
OUTPUT_PATH = fix_path("./projects/ios_data_transform/tests/temp/")
TEST_FILE_FOLDER = os.path.join(MODULE_PATH, "test_files")


def convert_mctd_files(f, out_path):
    fdata = ios.MCtdFile(filename=f, debug=False)
    if fdata.import_data():
        fdata.assign_geo_code(GEOGRAPHICAL_AREAS)
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
        fdata.assign_geo_code(GEOGRAPHICAL_AREAS)
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
        fdata.assign_geo_code(GEOGRAPHICAL_AREAS)
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
        fdata.assign_geo_code(GEOGRAPHICAL_AREAS)
        write_cur_ncfile(fix_path(out_path + f.split(os.path.sep)[-1] + ".nc"), fdata)
        # iod.utils.add_standard_variables(fix_path(out_path + f.split(os.path.sep)[-1] + '.nc')) #Ignore for now
    else:
        print("Unable to import data from file", fdata.filename)


def convert_any_files(f, out_path):
    print(f)
    fdata = ios.GenFile(filename=f, debug=False)

    if fdata.import_data():
        fdata.assign_geo_code(GEOGRAPHICAL_AREAS)
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

    def test_che_files(self):
        """Test to parse che files with the original method conversion"""
        files = glob(
            fix_path("./projects/ios_data_transform/tests/test_files/che/*.*"),
            recursive=True,
        )
        assert len(files) > 0, "No files found for conversion tests"
        for fn in files:
            convert_any_files(f=fn, out_path=OUTPUT_PATH)

    def test_drf_files(self):
        """Test to parse drf files to xarray and netCDF"""
        files = glob(
            fix_path("./projects/ios_data_transform/tests/test_files/drf/*.*"),
            recursive=True,
        )
        assert len(files) > 0, "No files found for conversion tests"
        for fn in files:
            convert_any_files(f=fn, out_path=OUTPUT_PATH)


@pytest.mark.parametrize(
    "file",
    glob(
        fix_path("./projects/ios_data_transform/tests/test_files/**/*.*"),
        recursive=True,
    ),
)
def test_all_ios_files(file):
    """Test to convert all the file types by using the general parser"""
    assert file, "No files found for conversion tests"
    if file.endswith(("geojson")):
        return
    convert_any_files(f=file, out_path=OUTPUT_PATH)


def run_script_on(ftype, raw_folder):
    """Run standard conversion script to the test files"""
    # redirect input and outputs to
    # tests file folder and temporary directory respectively.
    config = cioos_utils.read_config(
        os.path.join(MODULE_PATH, "..", f"config_{ftype}.json")
    )
    config.update(
        {
            "raw_folder": raw_folder,
            "nc_folder": OUTPUT_PATH,
            "geojson_file": os.path.join(MODULE_PATH, "..", f"ios_polygons.json"),
        }
    )
    opt = "all"
    flist = ios_data_transform_script.convert_files(config=config, opt=opt, ftype=ftype)
    assert len(flist) > 0, "No tests files detected"
    return flist


class TestIosScriptConversions(unittest.TestCase):
    def test_bot_script_conversion(self):
        run_script_on("bot", os.path.join(TEST_FILE_FOLDER, "bot"))

    def test_ctd_profiles_script_conversion(self):
        run_script_on("ctd", os.path.join(TEST_FILE_FOLDER, "ctd_profile"))

    def test_cur_script_conversion(self):
        run_script_on("cur", os.path.join(TEST_FILE_FOLDER, "current_meter"))

    def test_drf_script_conversion(self):
        run_script_on("drf", os.path.join(TEST_FILE_FOLDER, "drf"))

    def test_tob_script_conversion(self):
        run_script_on("tob", os.path.join(TEST_FILE_FOLDER, "tob"))

    def test_ane_script_conversion(self):
        run_script_on("ane", os.path.join(TEST_FILE_FOLDER, "ane"))

    def test_ubc_script_conversion(self):
        run_script_on("ubc", os.path.join(TEST_FILE_FOLDER, "ubc"))

    def test_loop_script_conversion(self):
        run_script_on("loop", os.path.join(TEST_FILE_FOLDER, "loop"))
