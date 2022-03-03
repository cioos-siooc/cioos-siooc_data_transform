__version__ = "0.1"
import glob
import json
import os

import numpy as np

import argparse

from odf_transform import parser as odf_parser
from odf_transform import attributes

from cioos_data_transform.utils.xarray_methods import standardize_dataset
import cioos_data_transform.parse.seabird as seabird
from cioos_data_transform.utils.utils import get_geo_code, read_geojson
import pandas as pd

from tqdm import tqdm
import logging

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(MODULE_PATH, "config.json")

# Log to log file
logging.captureWarnings(True)

logger = logging.getLogger()
logger.setLevel('INFO')
log_file = logging.FileHandler("odf_transform.log", encoding='UTF-8')
formatter = logging.Formatter("%(odf_file)s - %(asctime)s [%(levelname)s] %(processName)s %(name)s: %(message)s")
log_file.setFormatter(formatter)
log_file.setLevel(logging.WARNING)
logger.addHandler(log_file)

# Set up logging to console (errors only)
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
console.setFormatter(formatter)
logger.addHandler(console)

# Adapt Logger to have incorporated the odf_file name
logger = logging.LoggerAdapter(logger,{'odf_file':None})
seabird.logger = logging.LoggerAdapter(seabird.logger,{'odf_file': None})
attributes.logger = logging.LoggerAdapter(attributes.logger,{'odf_file': None})
odf_parser.logger = logging.LoggerAdapter(odf_parser.logger,{'odf_file': None})

def read_config(config_file):
    """Function to load configuration json file and vocabulary file."""
    # read json file with information on dataset etc.
    with open(config_file, encoding='UTF-8') as fid:
        config = json.load(fid)

    # If config.json is default in package set relative paths to module path
    if os.path.join(MODULE_PATH, "config.json") == DEFAULT_CONFIG_PATH:
        config["odf_path"] = os.path.join(MODULE_PATH, config["odf_path"][2:])
        config["output_path"] = os.path.join(MODULE_PATH, config["output_path"][2:])
        config["geojsonFileList"] = [
            os.path.join(MODULE_PATH, fpath[2:]) for fpath in config["geojsonFileList"]
        ]
        config["vocabularyFile"] = os.path.join(MODULE_PATH, config["vocabularyFile"])

    # Read Vocabulary file
    vocab = pd.read_csv(config["vocabularyFile"], index_col=["Vocabulary", "name"])
    config["vocabulary"] = vocab.fillna(np.nan).replace({np.nan: None})
    return config


def write_ctd_ncfile(
    odf_path, output_path=None, config=None, polygons={},
):
    """Method use to convert odf files to a CIOOS/ERDDAP compliant NetCDF format"""
    odf_file = os.path.basename(odf_path)
    log = {'odf_file': odf_file}

    # Update submodule LoggerAdapter
    seabird.logger.extra.update(log)
    attributes.logger.extra.update(log)
    odf_parser.logger.extra.update(log)
    logger.extra.update(log)

    # Parse the ODF file with the CIOOS python parsing tool
    metadata, raw_data = odf_parser.read(odf_path)

    # Review ODF data type compatible with odf_transform
    if metadata["EVENT_HEADER"]["DATA_TYPE"] not in ["CTD", "BT", "BOTL"]:
        logger.error(
            f'ODF_transform is not yet compatible with ODF Data Type: {metadata["EVENT_HEADER"]["DATA_TYPE"]}'
        )
        return

    # Let's convert to an xarray dataset
    ds = raw_data.to_xarray()

    # Write global attributes
    ds.attrs = config["global_attributes"]
    ds = attributes.global_attributes_from_header(ds, metadata)
    ds.attrs[
        "history"
    ] += f"# Convert ODF to NetCDF with cioos_data_trasform.odf_transform V {__version__}"
    ds.attrs["original_filename"] = odf_file
    ds.attrs["id"] = ds.attrs["original_filename"].replace(".ODF", "")

    # Add variables attributes from odf
    for var, attrs in metadata["variable_attributes"].items():
        if var in ds:
            ds[var].attrs = attrs

    # Handle ODF flag variables
    ds = odf_parser.odf_flag_variables(ds, config.get("flag_convention"))

    # Generate Variables from attributes
    ds = attributes.generate_variables_from_header(ds, metadata)

    # geographic_area
    ds["geographic_area"] = get_geo_code(
        [ds["longitude"].mean(), ds["latitude"].mean()], polygons
    )

    # Add Vocabulary attributes
    ds = odf_parser.get_vocabulary_attributes(
        ds,
        organizations=config["organisationVocabulary"],
        vocabulary=config["vocabulary"],
    )

    # Fix flag variables with some issues to map
    ds = odf_parser.fix_flag_variables(ds)

    if ds.attrs["instrument_manufacturer_header"].startswith("* Sea-Bird"):
        ds = seabird.add_seabird_calibration(
            ds, ds.attrs["instrument_manufacturer_header"], match_by="sdn_parameter_urn"
        )
        ds = seabird.update_attributes_from_seabird_header(
            ds, ds.attrs["instrument_manufacturer_header"]
        )

    # Add geospatial and geometry related global attributes
    # Just add spatial/time range as attributes
    initial_attrs = set(ds.attrs.keys())
    ds = standardize_dataset(ds)
    dropped_attrs = initial_attrs - ds.attrs.keys()
    if dropped_attrs:
        logging.info(f"Drop empty attributes: {dropped_attrs}")

    # Handle dimensions
    if ds.attrs["cdm_data_type"] == "Profile" and "index" in ds and "depth" in ds:
        ds = ds.swap_dims({"index": "depth"}).drop_vars("index")

    # Finally save the xarray dataset to a NetCDF file!!!
    if output_path == None:
        output_path = odf_path + ".nc"
    elif os.path.isdir(output_path):
        output_path = os.path.join(output_path,odf_file + ".nc")
    ds.to_netcdf(output_path)


def convert_odf_file(input):
    if type(input)== tuple:
        file, polygons, output_path, config = input
    logger.extra['odf_file'] = os.path.basename(file)
    try:
        write_ctd_ncfile(
                    odf_path=file,
                    polygons=polygons,
                    output_path=output_path,
                    config=config,
                )
    except Exception as e:
        logger.error(f"Failed to convert: {file}", exc_info=True)


def convert_odf_files(config, odf_files_list=[], output_path=""):
    """Principal method to convert multiple ODF files to NetCDF"""
    polygons = read_geojson_file_list(config["geojsonFileList"])

    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    pbar = tqdm(unit='file',desc='ODF Conversion To NetCDF: ', total=len(odf_files_list))
    for f in odf_files_list:
        logger.extra['odf_file'] = os.path.basename(f)
        pbar.set_description_str(f'Convert ODF ({os.path.basename(f)}): ')
        pbar.update(1)
        convert_odf_file((f,polygons,output_path,config))

def read_geojson_file_list(file_list):
    """Read geojson files"""
    polygons_dict = {}
    for fname in file_list:
        polygons_dict.update(read_geojson(fname))
    return polygons_dict


#
# make this file importable
#
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

    if not odf_path:
        raise Exception("No odf_path")
    elif os.path.isdir(odf_path):
        odf_files_list = glob.glob(odf_path + "/**/*.ODF", recursive=True)
    elif os.path.isfile(odf_path):
        odf_files_list = [odf_path]
    else:
        odf_files_list = glob.glob(odf_path, recursive=True)
    print(f"Convert {len(odf_files_list)} ODF files")
    convert_odf_files(config, odf_files_list, output_path)
