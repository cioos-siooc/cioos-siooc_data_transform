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

# Read Vocabulary file
for vocab_file in config['vocabularyFileList']:
    config.update({"vocabulary": {}})
    with open(vocab_file) as fid:
        vocab = json.load(fid)
    config["vocabulary"].update(vocab)

convert_test_files(config)
