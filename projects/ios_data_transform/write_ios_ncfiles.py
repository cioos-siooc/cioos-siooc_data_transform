from glob import glob
import os
import logging
import argparse
import json

import cioos_data_transform.IosObsFile as ios
from tqdm import tqdm

logging.basicConfig(filename="drf.log", level=logging.DEBUG)
logger = logging.getLogger()


def read_config(config_input):
    """Read the configuration file"""
    if config_input is None:
        config_input = os.path.join(os.path.dirname(__file__), "config_default.json")

    if isinstance(config_input, str):
        if os.path.exists(config_input):
            logger.info("Parse json file config")
            with open(config_input, "r", encoding="UTF-8") as config_file:
                return json.load(config_file)
        logger.info("Parse json string config")
        return json.loads(config_input)
    logger.info("Return config dictionary")
    return config_input


def convert_file_to_netcdf(input_path, output_path, config_input=None, overwrite=False):

    config = read_config(config_input)
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
    fdata = ios.GenFile(filename=input_path, debug=False)

    if fdata.import_data():
        fdata.assign_geo_code(config["geojson_file"])
        fdata.add_ios_vocabulary()

        ds = fdata.to_xarray()
        ds.attrs.update(config.get("global_attributes"))
        ds.to_netcdf(output_path)


def run_batch_conversion(
    input_path=None, output_path=None, config_input=None, recursive=None
):

    config = read_config(config_input)
    input_path = input_path or config["input_path"]
    output_path or config.get("output_path")
    recursive = recursive or config.get("recursive", True)

    files = glob(input_path, recursive=recursive)
    for file in tqdm(files, unit="file", desc="Convert IOS CTD to ODF"):
        try:
            convert_file_to_netcdf(file, output_path, config_input=config)
        except Exception as e:
            logger.exception("Failed to read %s", file)


# sourcery skip: avoid-builtin-shadow
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
