from write_ctd_ncfile_modules import *

CONFIG_PATH = fix_path("./config.json")
TEST_FILES_PATH = fix_path("./test/test_files/")
TEST_FILES_OUTPUT = fix_path("./test/temp/")


config = read_config(CONFIG_PATH)

# read geojson files
polygons_dict = {}
for fname in config["geojsonFileList"]:
    polygons_dict.update(read_geojson(fname))
config.update({"polygons_dict": polygons_dict})
# print(polygons_dict)
config.update(
    {
        "TEST_FILES_PATH": TEST_FILES_PATH,
        "TEST_FILES_OUTPUT": TEST_FILES_OUTPUT,
    }
)

convert_test_files(config)
