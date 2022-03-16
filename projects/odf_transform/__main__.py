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

# Log
logger = logging.getLogger()
logger.setLevel("INFO")

# Log issues with covnersion
log_file = logging.FileHandler("odf_transform.log", encoding="UTF-8")
formatter = logging.Formatter(
    "%(odf_file)s - %(asctime)s [%(levelname)s] %(processName)s %(name)s: %(message)s"
)
log_file.setFormatter(formatter)
log_file.setLevel(logging.WARNING)
logger.addHandler(log_file)

# Set logger to log variable names
var_log_file = logging.FileHandler("odf_transform_variables.log", encoding="UTF-8")
formatter = logging.Formatter("%(odf_file)s - %(asctime)s: %(message)s")
var_log_file.setFormatter(formatter)
var_log_file.setLevel(logging.INFO)
var_log_file.addFilter(logging.Filter(name="odf_transform.process"))
logger.addHandler(var_log_file)

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
        default=None,
        help="Enter the folder to write your NetCDF files to (Default: next to the original ODF file).",
        required=False,
    )
    parser.add_argument(
        "--n_workers",
        type=str,
        dest="n_workers",
        default=4,
        help="Amount of workers used while processing in parallel",
        required=False,
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite all NetCDF files.",
        required=False,
    )

    args = parser.parse_args()
    config = read_config(args.config_path)
    odf_path = args.odf_path
    output_path = args.output_path

    # Retrieve files to convert
    print("Retrieve files to process")
    if not odf_path:
        raise Exception("No odf_path")
    elif os.path.isdir(odf_path):
        odf_files_list = glob.glob(odf_path + "/**/*.ODF", recursive=True)
    elif os.path.isfile(odf_path):
        odf_files_list = [odf_path]
    else:
        odf_files_list = glob.glob(odf_path, recursive=True)
    print(f"Convert {len(odf_files_list)} ODF files")

    # Review keep files that needs an update only
    if args.overwrite:
        logger.info(f"Overwrite all {len(odf_files_list)}")
    elif output_path == None or os.path.isdir(output_path):
        overwrite_list = []
        new_list = []
        for file in odf_files_list:
            filename = os.path.basename(file)
            ncfile = (
                file + ".nc"
                if output_path == None
                else os.path.join(output_path, filename + ".nc")
            )
            if not os.path.exists(ncfile):
                new_list.append(file)
            elif os.path.getmtime(ncfile) < os.path.getmtime(file):
                overwrite_list.append(file)
        print(
            f"Create {len(new_list)}/{len(odf_files_list)} files - "
            + f"Overwrite {len(overwrite_list)}/{len(odf_files_list)} files"
        )
        odf_files_list = overwrite_list
        odf_files_list += new_list

    # No file to convert
    if odf_files_list == []:
        print("No file to convert")
        quit()

    # Get Polygon regions
    polygons = read_geojson_file_list(config["geojsonFileList"])

    # Generate inputs and run conversion with multiprocessing
    inputs = [(file, polygons, output_path, config) for file in odf_files_list]
    tqdm_dict = {
        "total": len(inputs),
        "desc": "ODF Conversion to NetCDF: ",
        "unit": "file",
    }
    if len(inputs) > 100:
        print(f"Run ODF conversion in multiprocessing on {args.n_workers} workers")
        with Pool(args.n_workers) as p:
            r = list(tqdm(p.imap(convert_odf_file, inputs), **tqdm_dict))
    elif 0 < len(inputs) < 100:
        print("Run ODF Conversion")
        for item in tqdm(inputs, **tqdm_dict):
            convert_odf_file(item)

