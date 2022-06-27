import argparse
import glob
import logging
import os
import re
from multiprocessing import Pool

import pandas as pd
from tqdm import tqdm

from odf_transform.process import (
    convert_odf_file,
    eval_config_input,
    read_config,
)

tqdm.pandas()


MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(MODULE_PATH, "config.json")

if __name__ == "__main__":
    # Log to log file
    logging.captureWarnings(True)

    # Log
    logger = logging.getLogger()
    logger.setLevel("INFO")

    # Log issues with conversion
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

    logger = logging.LoggerAdapter(logger, {"odf_file": None})


def input_from_program_logs(program_log_path, files, global_config):
    """Generate input based on the program logs available

    input_from_program_logs compile all the different program logs available within the directory
    and match each files that neeeds a conversion to the appropriate mission. For each mission,
    it updates the configuration global attributes to include any extra attributes available
    within the log.

    A list of inputs is then generated which can be run by the ODF conversion tool..

    Args:
        program_log_path (string):
        files (list): List of files
        polygons (dict): dictionary of geojson regions
        output_path (str): path to output files to
        config (dict)): [description]

    Returns:
        list: list of inputs to run the conversion for each files and and their
        specific mission attributes.
    """

    def generate_mission_config(mission_attrs):
        """Generate mission specific configuration"""
        mission_config = global_config.copy()
        mission_config["global_attributes"] = {
            **mission_config["global_attributes"],
            **mission_attrs.dropna().drop(["mission", "mission_file_list"]).to_dict(),
        }
        return mission_config

    # Load the different logs and map the list of files available to the programs
    program_logs = [
        item
        for item in os.listdir(program_log_path)
        if os.path.isfile(os.path.join(program_log_path, item))
    ]

    df_logs = pd.DataFrame()
    logger.info(f"Load Program Logs: {program_logs}")
    for log in program_logs:
        # Read program logs
        df_log = pd.read_csv(os.path.join(program_log_path, log))

        # Extract the program name from the file name
        if "program" not in df_log:
            df_log["program"] = log.rsplit(".", 1)[0]

        # Append to previous logs
        df_logs = df_logs.append(df_log, ignore_index=True)

    # Review duplicated mission input in log
    if df_logs["mission"].duplicated().any():
        duplicated_mission = df_logs.loc[df_logs["mission"].duplicated()]
        logger.error(f"Duplicated mission inputs were detected: {duplicated_mission}")
        return None

    # Ignore files that the mission isn't listed
    search_regexp = "|".join(df_logs["mission"].tolist())
    files = [file for file in files if re.search(search_regexp, file)]
    if len(files) == 0:
        return None
    logger.info(f"{len(files)} files are associated to a mission.")

    logger.info("Group files available for conversion by missions")
    df_logs["mission_file_list"] = df_logs.progress_apply(
        lambda x: [file for file in files if re.search(x["mission"], file)] or None,
        axis=1,
    )
    # Drop Mission with no files available
    df_logs = df_logs.dropna(subset=["mission_file_list"])

    logger.info(
        f"Generate Mission Specific Configuration for {len(files)} files associated "
        + f"with {len(df_logs)} missions"
    )
    df_logs["mission_config"] = df_logs.progress_apply(generate_mission_config, axis=1)

    # Generate input for each files
    convert_odf_inputs = []
    logger.info("Retrieve mission matching files")
    for _, df_mission in df_logs.iterrows():
        convert_odf_inputs += [
            (mission_file, df_mission["mission_config"])
            for mission_file in df_mission["mission_file_list"]
        ]

    return convert_odf_inputs


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
        help="Output directory (Default: next to the original ODF file).",
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
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Output INFO to console",
        required=False,
    )
    args = parser.parse_args().__dict__

    # Output info to console if requested
    if args["verbose"]:
        console.setLevel(logging.INFO)

    # Read config and overwrite config with inputs to console
    config = read_config(args["config_path"])
    config.update({key: value for key, value in args.items() if value})

    # Handle Input FIles
    logger.info("Retrieve files to process")
    fileDir = eval_config_input(config["fileDir"])
    if os.path.isfile(fileDir):
        odf_files_list = [fileDir]
    elif os.path.isdir(fileDir):
        odf_files_list = glob.glob(f"{fileDir}/**/*.ODF", recursive=config["recursive"])

    else:
        odf_files_list = glob.glob(fileDir, recursive=config["recursive"])

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
    logger.info(f"{len(odf_files_list)} ODF files are available")

    # Review keep files that needs an update only
    output_path = config["output_path"]

    if config["overwrite"]:
        # overwrite all files
        logger.info(f"Overwrite all {len(odf_files_list)}")
    elif os.path.isfile(output_path):
        logger.info(f"Overwrite file: {output_path}")
    else:
        # Get directory where all the files should be outputted
        if output_path is None:
            search_output_path = fileDir
        else:
            # Start at head of a fstring path or just the output_path
            search_output_path = output_path.split("{", 1)[0]

        # Get files available in output_path
        search_output_path_files = glob.glob(
            f"{search_output_path}/**/*.ODF.nc", recursive=True
        )

        outputted_files = {
            os.path.basename(file): {
                "path": file,
                "last_modified": os.path.getmtime(file),
            }
            for file in search_output_path_files
        }

        # Output new file if not netcdf equivalent exist or netcdf is older than odf
        overwrite_list = []
        new_list = []
        for file in odf_files_list:
            filename = os.path.basename(file)
            ncfile = f"{filename + config['addFileNameSuffix']}.nc"

            if ncfile not in outputted_files:
                new_list.append(file)
            elif outputted_files[ncfile]["last_modified"] < os.path.getmtime(file):
                # source file is more recent than the corresponding netcdf
                overwrite_list.append(file)
        logger.info(
            f"Create {len(new_list)}/{len(odf_files_list)} files - "
            + f"Overwrite {len(overwrite_list)}/{len(odf_files_list)} files"
        )
        odf_files_list = overwrite_list
        odf_files_list += new_list

    # Generate inputs
    if config["program_logs_path"]:
        inputs = input_from_program_logs(
            config["program_logs_path"],
            odf_files_list,
            config,
        )
    else:
        inputs = [(file, config) for file in odf_files_list]

    if inputs:
        logger.info(f"{len(inputs)} files will be converted")
    else:
        logger.info("No file to convert")
        quit()

    tqdm_dict = {
        "total": len(inputs),
        "desc": "ODF Conversion to NetCDF: ",
        "unit": "file",
    }
    if len(inputs) > 100:
        logger.info(
            f"Run ODF conversion in multiprocessing with {config['n_workers']} workers"
        )
        with Pool(config["n_workers"]) as p:
            r = list(tqdm(p.imap(convert_odf_file, inputs), **tqdm_dict))
    elif 0 < len(inputs) < 100:
        print("Run ODF Conversion")
        for item in tqdm(inputs, **tqdm_dict):
            convert_odf_file(item)
