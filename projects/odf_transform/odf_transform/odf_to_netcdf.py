import glob
import json
import os
import traceback
import numpy as np

import shutil
import argparse

import odf_parser.odf_parser as odf
import cioos_data_transform.utils.xarray_methods as xarray_methods
from cioos_data_transform.utils.utils import fix_path
from cioos_data_transform.utils.utils import get_geo_code, read_geojson
import pandas as pd

import logging

# Log to log file
logging.captureWarnings(True)
logging.basicConfig(filename="odf_transform.log", level=logging.INFO)
logger = logging.getLogger()

# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# set a format which is simpler for console use
formatter = logging.Formatter("[%(levelname)-8s] %(message)s")
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger("").addHandler(console)

CONFIG_PATH = fix_path("./config.json")


def read_config(config_file):
    """Function to load configuration json file and vocabulary file."""
    # read json file with information on dataset etc.
    with open(config_file) as fid:
        config = json.load(fid)

    # Read Vocabulary file
    if config["vocabularyFile"] and config["vocabularyFile"].endswith("csv"):
        vocab = pd.read_csv(config["vocabularyFile"], index_col=["Vocabulary", "name"])
        config["vocabulary"] = vocab.fillna(np.nan).replace({np.nan: None})
    return config


def write_ctd_ncfile(
    odf_path, output_path=None, config=None, polygons={},
):
    """Method use to convert odf files to a CIOOS/ERDDAP compliant NetCDF format"""
    print(os.path.split(odf_path)[-1])
    # Parse the ODF file with the CIOOS python parsing tool
    metadata, raw_data = odf.read(odf_path)

    # Let's convert to an xarray dataset
    ds = raw_data.to_xarray()

    file_id = odf_path.split("\\")[-1]
    ds["file_id"] = file_id
    ds.attrs["filename"] = file_id

    # Write global attributes
    ds.attrs.update(config["global_attributes"])  # From the config file
    ds.attrs.update(odf.global_attributes_from_header(metadata))  # From ODF header

    # Add variables attributes from odf
    for var, attrs in metadata["variable_attributes"].items():
        if var in ds:
            ds[var].attrs = attrs

    ds = odf.odf_flag_variables(ds, config.get("flag_convention"))
    ds = odf.generate_variables_from_header(
        ds, metadata, config["cdm_data_type"]
    )  # From ODF header

    # geographic_area
    ds["geographic_area"] = get_geo_code(
        [ds["longitude"].mean(), ds["latitude"].mean()], polygons
    )

    # Add Vocabulary attributes
    ds = odf.get_vocabulary_attributes(
        ds,
        organizations=config["organisationVocabulary"],
        vocabulary=config["vocabulary"],
    )

    # Add geospatial and geometry related global attributes
    # Just add spatial/time range as attributes
    ds = xarray_methods.get_spatial_coverage_attributes(ds)
    # Retrieve geometry information
    ds = xarray_methods.derive_cdm_data_type(ds, config["cdm_data_type"])
    # Add encoding information to xarray
    ds = xarray_methods.convert_variables_to_erddap_format(ds)
    # Assign the right dimension
    ds = xarray_methods.define_index_dimensions(ds)

    # Standardize variable attributes
    ds = xarray_methods.standardize_variable_attributes(ds)

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
        except KeyError as e:
            logger.error(f"{f} is missing {e.args}")
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
    config = read_config(CONFIG_PATH)
    parser = argparse.ArgumentParser(
        prog="odf_to_netcdf", description="Convert ODF files to NetCDF"
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
        help="Enter the folder to write your NetCDF files to." + "Defaults to 'output'",
        required=False,
    )
    parser.add_argument(
        "-subdir",
        action="store_true",
        dest="subdir",
        help="Look in subdirectories'",
        required=False,
    )

    args = parser.parse_args()
    odf_path = args.odf_path
    output_path = args.output_path + "/"
    subdir = args.subdir

    odf_files_list = []

    if not odf_path:
        raise Exception("No odf_path")

    if os.path.isdir(odf_path) and subdir:
        # Get all ODF within the subdirectories
        for root, dirs, files in os.walk(odf_path):
            for file in files:
                if file.endswith(".ODF"):
                    odf_files_list.append(os.path.join(root, file))

    elif os.path.isdir(odf_path):
        odf_files_list = glob.glob(odf_path + "/*.ODF")
    elif os.path.isfile(odf_path):
        odf_files_list = [odf_path]
    print(f"Convert {len(odf_files_list)} ODF files")
    convert_odf_files(config, odf_files_list, output_path)
