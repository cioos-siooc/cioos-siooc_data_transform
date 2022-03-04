__version__ = "0.1.0"
from odf_transform.process import *

import os
import glob
import argparse

from multiprocessing import Pool
from tqdm import tqdm

import logging

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(MODULE_PATH, "config.json")

# Log to log file
logging.captureWarnings(True)

logger = logging.getLogger()
logger.setLevel("INFO")
log_file = logging.FileHandler("odf_transform.log", encoding="UTF-8")
formatter = logging.Formatter(
    "%(odf_file)s - %(asctime)s [%(levelname)s] %(processName)s %(name)s: %(message)s"
)
log_file.setFormatter(formatter)
log_file.setLevel(logging.WARNING)
logger.addHandler(log_file)

# Set up logging to console (errors only)
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
console.setFormatter(formatter)
logger.addHandler(console)

if __name__ == "__main__":
    config = read_config(DEFAULT_CONFIG_PATH)
    parser = argparse.ArgumentParser(
        prog="odf_transform", description="Convert ODF files to NetCDF"
    )
    parser.add_argument(
        "-c",
        type=str,
        dest="config_path",
        default=DEFAULT_CONFIG_PATH,
        help="Path to config file to use for the odf conversion. config.json",
    )
    parser.add_argument(
        "-i",
        type=str,
        dest="odf_path",
        default=config.get("odf_path"),
        help="ODF file or directory with ODF files. Recursive",
    )
    parser.add_argument(
        "-o",
        type=str,
        dest="output_path",
        default=config.get("output_path", "./output/"),
        help="Enter the folder to write your NetCDF files to. Next to the original ODF file.'",
        required=False,
    )

    args = parser.parse_args()
    config = read_config(args.config_path)
    odf_path = args.odf_path
    output_path = args.output_path

    # Retrieve files to convert
    if not odf_path:
        raise Exception("No odf_path")
    elif os.path.isdir(odf_path):
        odf_files_list = glob.glob(odf_path + "/**/*.ODF", recursive=True)
    elif os.path.isfile(odf_path):
        odf_files_list = [odf_path]
    else:
        odf_files_list = glob.glob(odf_path, recursive=True)
    print(f"Convert {len(odf_files_list)} ODF files")

    # Get Polygon regions
    polygons = read_geojson_file_list(config["geojsonFileList"])

    # Generate inputs and run conversion with multiprocessing
    inputs = [(file, polygons, output_path, config) for file in odf_files_list]
    with Pool(3) as p:
        r = list(
            tqdm(
                p.imap(convert_odf_file, inputs),
                total=len(inputs),
                desc="ODF Conversion to NetCDF: ",
                unit="file",
            )        )

