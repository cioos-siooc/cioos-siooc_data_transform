import argparse
import json
import logging
import logging.config
import os
from glob import glob

import cioos_data_transform.IosObsFile as ios
from cioos_data_transform.utils import read_config
from tqdm import tqdm

log_config_path = os.path.join(os.path.dirname(__file__), "log_config.ini")
logger = logging.getLogger(None if __name__ == "__main__" else __name__)
MODULE_PATH = os.path.dirname(__file__)
logger = logging.LoggerAdapter(logger, {"file": None})


def parse_ios_file(input_path, output_path, config_input=None, overwrite=False):
    """Parse IOS file with default configuration associated with the file type."""
    ftype = input_path.rsplit(".")[1]
    if isinstance(config_input, str):
        config = read_config(config_input, ftype)
    else:
        config = config_input
    overwrite = overwrite or config.get("overwrite")

    # Generate output path
    original_filename = os.path.basename(input_path)
    if output_path is None:
        output_path = f"{input_path}.nc"
    elif os.path.isdir(output_path):
        output_path = os.path.join(output_path, f"{original_filename}.nc")

    # overwrite?
    if os.path.exists(output_path) and not overwrite:
        logger.warning("File already exists")
        return

    logger.debug("Parse file: %s", input_path)
    return ios.GenFile(filename=input_path, debug=False)


def write_ios_ncfile(output_path, fdata, config):
    """Save parsed ios file data to a netcdf format"""
    fdata.add_ios_vocabulary()

    ds = fdata.to_xarray()
    ds.attrs.update(config.get("global_attributes"))
    ds.to_netcdf(output_path)


def run_batch_conversion(
    input_path=None, output_path=None, config_input=None, recursive=None
):

    config = read_config(config_input)
    input_path = input_path or os.path.join(config["raw_folder"], config["files"])
    output_path = output_path or config.get("nc_folder")
    recursive = recursive or config.get("recursive", True)

    files = glob(input_path, recursive=recursive)
    for file in tqdm(files, unit="file", desc="Convert IOS files to NetCDF"):
        try:
            fdata = parse_ios_file(file, output_path, config_input=config)
            if not fdata.import_data():
                logger.error("Failed to parseIOS File %s", file)
                continue

            fdata.assign_geo_code(
                config.get("geographical_areas")
                or os.path.join(MODULE_PATH, "samples", "ios_polygons.geojson")
            )
            write_ios_ncfile(output_path, fdata, config)

        except Exception as e:
            logger.exception("Failed to read %s", file, extra={"file": file})


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog="Convert IOS files to NetCDF",
        description="Run conversion of netcdf files",
    )
    parser.add_argument(
        "-c",
        "--config_input",
        default=None,
        help="Path to configuration file or json string",
    )
    parser.add_argument(
        "-i",
        "--input_path",
        default=None,
        help="Search path to be use to retrieve files to input",
    )
    parser.add_argument(
        "-o",
        "--output_path",
        default=None,
        help='Output path where to save netcdf files to input, (default: original file +".nc")',
    )
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help='Output path where to save netcdf files to input, (default: original file +".nc")',
    )
    run_batch_conversion(**parser.parse_args().__dict__)
