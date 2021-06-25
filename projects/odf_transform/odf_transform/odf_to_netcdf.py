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
    odf_path,
    output_path,
    config=None,
    polygons={},
):
    """Method use to convert odf files to a CIOOS/ERDDAP compliant NetCDF format"""
    print(os.path.split(odf_path)[-1])
    # Parse the ODF file with the CIOOS python parsing tool
    metadata, raw_data = odf.read(odf_path)

    # Let's convert to an xarray dataset
    ds = raw_data.to_xarray()

    # Write global attributes
    ds.attrs.update(config["global_attributes"])  # From the config file
    ds.attrs.update(odf.global_attributes_from_header(metadata))  # From ODF header

    # Add New Variables
    metadata["variable_attributes"] = odf.odf_flag_variables(
        metadata["variable_attributes"], config.get("flag_convention")
    )
    ds = odf.generate_variables_from_header(
        ds, metadata, config["cdm_data_type"]
    )  # From ODF header
    # geographic_area
    ds["geographic_area"] = get_geo_code(
        [ds["longitude"].mean(), ds["latitude"].mean()], polygons
    )

    # Add file name as a variable
    cdm_data_type = config.get("cdm_data_type")
    file_id = os.path.split(odf_path)[-1]
    ds["file_id"] = file_id
    if cdm_data_type in ["Profile", "TimeSeries", "Trajectory", "TimeSeriesProfiles"]:
        ds[cdm_data_type.lower() + "_id"] = file_id
    else:
        raise ValueError("Unknown cdm_data_type: {0}".format(cdm_data_type))

    # Add Vocabulary attributes
    var_attributes = odf.get_vocabulary_attributes(
        metadata["variable_attributes"],
        organizations=config["organisationVocabulary"],
        vocabulary=config["vocabulary"],
        global_attributes=ds.attrs
    )

    # Add variable attributes to ds variables
    for var, attrs in var_attributes.items():
        if var in ds:
            ds[var].attrs.update(attrs)
        else:
            ds[var] = ds[attrs['source']].copy()
            ds[var].attrs.update(attrs)

    ds = xarray_methods.add_variable_attributes(ds)

    # Rename variables if name attribute exist
    variable_to_rename = {var: ds[var].attrs['name'] for var in ds if 'name' in ds[var].attrs}
    if variable_to_rename:
        ds = ds.rename(variable_to_rename)

    # Generate extra variables (BODC, Derived)
    ds = xarray_methods.generate_bodc_variables(ds)

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
    for var in ds.keys():
        sorted_attributes = xarray_methods.standardize_variable_attributes(
            ds[var].values, ds[var].attrs
        )
        if sorted_attributes:
            ds[var].attrs = sorted_attributes

    # Finally save the xarray dataset to a NetCDF file!!!
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
            # Copy problematic files to a subfolder
            if not os.path.isdir(os.path.join(os.path.dirname(f), "failed")):
                os.mkdir(os.path.join(os.path.dirname(f), "failed"))
            shutil.copy(
                f,
                os.path.join(os.path.dirname(f), "failed", os.path.split(f)[-1]),
            )

            print("***** ERROR***", f)
            print(e)
            print(traceback.print_exc())


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

    args = parser.parse_args()
    odf_path = args.odf_path
    output_path = args.output_path + "/"

    odf_files_list = []

    if not odf_path:
        raise Exception("No odf_path")

    if os.path.isdir(odf_path):
        odf_files_list = glob.glob(odf_path + "/*.ODF")
    elif os.path.isfile(odf_path):
        odf_files_list = [odf_path]

    convert_odf_files(config, odf_files_list, output_path)
