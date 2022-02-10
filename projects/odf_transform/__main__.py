import glob
import json
import os

import numpy as np

import argparse

from odf_transform import parser as odf_parser
from odf_transform import attributes

import cioos_data_transform.utils.xarray_methods as xarray_methods
from cioos_data_transform.utils.utils import fix_path
from cioos_data_transform.utils.utils import get_geo_code, read_geojson
import pandas as pd

import logging

MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(MODULE_PATH, "config.json")

# Log to log file
logging.captureWarnings(True)
logging.basicConfig(filename="odf_transform.log", level=logging.WARNING)
formatter = logging.Formatter(
    "%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s"
)
logger = logging.getLogger()

# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter("[%(levelname)-8s] %(message)s")
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger("").addHandler(console)


def read_config(config_file):
    """Function to load configuration json file and vocabulary file."""
    # read json file with information on dataset etc.
    with open(config_file) as fid:
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
    print(os.path.split(odf_path)[-1])
    # Parse the ODF file with the CIOOS python parsing tool
    metadata, raw_data = odf_parser.read(odf_path)

    # Let's convert to an xarray dataset
    ds = raw_data.to_xarray()

    # Write global attributes
    ds.attrs.update(
        attributes.global_attributes_from_header(metadata)
    )  # From ODF header
    ds.attrs.update(config["global_attributes"])  # From the config file
    ds.attrs["filename"] = os.path.basename(odf_path)
    ds.attrs["id"] = ds.attrs["filename"].replace(".ODF", "")

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

    # Add geospatial and geometry related global attributes
    # Just add spatial/time range as attributes
    ds = xarray_methods.get_spatial_coverage_attributes(ds)
    # Add encoding information to xarray
    ds = xarray_methods.convert_variables_to_erddap_format(ds)
    # Standardize variable attributes
    ds = xarray_methods.standardize_variable_attributes(ds)

    # Handle dimensions
    if ds.attrs["cdm_data_type"] == "Profile" and "index" in ds and "depth" in ds:
        ds = ds.swap_dims({"index": "depth"}).drop_vars("index")

    # Convert to iso date time and drop empty attributes
    initial_attrs = set(ds.attrs.keys())
    ds.attrs = {
        key: value
        if type(value) != pd.Timestamp
        else value.strftime("%Y-%m-%dT%H:%M:%SZ")
        for key, value in ds.attrs.items()
        if value not in [None, ""]
    }
    dropped_attrs = initial_attrs - ds.attrs.keys()
    if dropped_attrs:
        logging.info(f"Drop empty attributes: {dropped_attrs}")

    # Finally save the xarray dataset to a NetCDF file!!!
    if output_path == None:
        output_path = odf_path + ".nc"
    ds.to_netcdf(output_path)


def convert_odf_files(config, odf_files_list=[], output_path=""):
    """Principal method to convert multiple ODF files to NetCDF"""
    polygons = read_geojson_file_list(config["geojsonFileList"])

    if not os.path.isdir(output_path):
        os.mkdir(output_path)

    for f in odf_files_list:
        try:
            print(f)
            write_ctd_ncfile(
                odf_path=f,
                polygons=polygons,
                output_path=output_path + "{}.nc".format(os.path.basename(f)),
                config=config,
            )
        except Exception as e:
            logger.error(f"Failed to convert: {f}", exc_info=True)


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
    output_path = args.output_path + "/"

    if not odf_path:
        raise Exception("No odf_path")

    elif os.path.isdir(odf_path):
        odf_files_list = glob.glob(odf_path + "/**/*.ODF", recursive=True)
    elif os.path.isfile(odf_path):
        odf_files_list = [odf_path]
    print(f"Convert {len(odf_files_list)} ODF files")
    convert_odf_files(config, odf_files_list, output_path)
