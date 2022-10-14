from glob import glob
import os
import logging

import cioos_data_transform.IosObsFile as ios
from tqdm import tqdm

logger = logging.getLogger(__name__)
GEOJSON_AREAS_PATH = "/Users/jessybarrette/repo/cioos-siooc_data_transform/projects/ios_data_transform/samples/ios_polygons.geojson"


def convert_any_files(f, out_path):
    print(f)
    fdata = ios.GenFile(filename=f, debug=False)

    if fdata.import_data():
        fdata.assign_geo_code(GEOJSON_AREAS_PATH)
        fdata.add_ios_vocabulary()
        ds = fdata.to_xarray()

        ds.to_netcdf(out_path)


files = glob("/Users/jessybarrette/repo/ios_data_temp/**/*.*", recursive=True)
# files = ["/Users/jessybarrette/repo/ios_data_temp/bot/2020-005-0282.che"]
for file in tqdm(files, unit="file", desc="Convert IOS CTD to ODF"):
    if file.endswith(".nc"):
        continue
    try:
        convert_any_files(file, f"{file}.nc")
    except Exception as e:
        logger.error("Failed to read %s", file)
        with open("failed_files.txt", "a") as file_handle:
            file_handle.write(f"{file} -> {e}\n")
