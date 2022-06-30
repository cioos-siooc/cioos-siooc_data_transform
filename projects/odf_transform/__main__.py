"""Main module that is when odf_convert is used via the command line interface"""
import argparse
import logging
import os

from tqdm import tqdm

from odf_transform.process import (
    read_config,
    run_odf_conversion_from_config,
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
        "%(file)s - %(asctime)s [%(levelname)s] %(processName)s %(name)s: %(message)s"
    )
    log_file.setFormatter(formatter)
    log_file.setLevel(logging.WARNING)
    logger.addHandler(log_file)

    # Set logger to log variable names
    var_log_file = logging.FileHandler("odf_transform_variables.log", encoding="UTF-8")
    formatter = logging.Formatter("%(file)s - %(asctime)s: %(message)s")
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


if __name__ == "__main__":
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

    run_odf_conversion_from_config(config)
