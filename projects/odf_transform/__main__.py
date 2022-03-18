__version__ = "0.1.0"
from odf_transform.process import *

import os
import glob
import argparse

from multiprocessing import Pool
from tqdm import tqdm
import re

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


def input_from_program_log(program_log_path, files, polygons, output_path, config):
    # Load the different logs and map the list of files available to the programs
    program_logs = [
        item
        for item in os.listdir(program_log_path)
        if os.path.isfile(os.path.join(program_log_path, item))
    ]
    inputs = []
    for log in program_logs:
        df_log = pd.read_csv(os.path.join(program_log_path, log)).set_index("mission")
        # Add program name based on filename
        program = log.rsplit(".", 1)[0]
        df_log["program"] = program
        logger.info(f"Retrieve files from {program}")
        for mission, attrs in tqdm(
            df_log.iterrows(), unit="mission", total=df_log.shape[0], desc=program
        ):
            # Retrieve the files associated with this program
            mission_files = [
                file
                for file in files
                if re.search(mission.strip(), os.path.basename(file))
            ]
            logger.info(f"{len(mission_files)} were matched to the mission {mission}")
            if mission_files == []:
                continue
            # Make a copy of the configuration
            mission_config = config.copy()
            mission_config["global_attributes"].update(dict(attrs))
            for mission_file in mission_files:
                inputs.append((mission_file, polygons, output_path, mission_config))

    return inputs


def get_input(item):
    vargs = vars(args)
    if vargs.get(item):
        return vargs.get(item)
    else:
        return config[item]


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
        dest="fileDir",
        default=None,
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
        "-p",
        type=str,
        dest="program_logs_path",
        help="Path to program logs to ingest",
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

    # Handle output_path
    if get_input("output_path"):
        config["output_path"] = args.output_path

    # Handle Input FIles
    print("Retrieve files to process")
    fileDir = get_input("fileDir")
    if os.path.isfile(fileDir):
        odf_files_list = [fileDir]
    elif os.path.isdir(fileDir):
        odf_files_list = glob.glob(
            fileDir + "/**/*.ODF", recursive=get_input("recursive")
        )
    else:
        odf_files_list = glob.glob(fileDir, recursive=get_input("recursive"))

    # Filter results with regex
    if config["fileNameRegex"]:
        odf_files_list = [
            file
            for file in odf_files_list
            if re.match(config["fileNameRegex"], os.path.basename(file))
        ]
    if config["pathRegex"]:
        odf_files_list = [
            file for file in odf_files_list if re.match(config["pathRegex"], file)
        ]
    print(f"{len(odf_files_list)} ODF files are available")

    # Review keep files that needs an update only
    output_path = config["output_path"]

    if get_input("overwrite"):
        # overwrite all files
        logger.info(f"Overwrite all {len(odf_files_list)}")
    elif output_path == None or os.path.isdir(output_path):
        # Consider file if not netcdf equivalent exist or netcdf is older than odf
        overwrite_list = []
        new_list = []
        for file in odf_files_list:
            filename = os.path.basename(file)
            ncfile = (
                file + ".nc"
                if output_path == None
                else os.path.join(
                    output_path, filename + config["addFileNameSuffix"] + ".nc"
                )
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

    # Generate inputs
    if get_input("program_logs_path"):
        inputs = input_from_program_log(
            get_input("program_logs_path"),
            odf_files_list,
            polygons,
            output_path,
            config,
        )
    else:
        inputs = [(file, polygons, output_path, config) for file in odf_files_list]

    logger.info(f"{len(inputs)} files will be converted")

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

