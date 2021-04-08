import glob
import json
import os
import traceback
import re
import shutil

import xarray as xr
import utils.odf_parser as odf
import cioos_data_transform.utils.xarray_methods as xarray_methods
from cioos_data_transform.utils.utils import fix_path
from cioos_data_transform.utils.utils import get_geo_code, read_geojson
import datetime as dt
import pandas as pd


def read_config(config_file):
    # read json file with information on dataset etc.
    with open(config_file) as fid:
        config = json.load(fid)

        # Read Vocabulary file
        for vocab_file in config["vocabularyFileList"]:
            config.update({"vocabulary": {}})
            with open(vocab_file) as fid:
                vocab = json.load(fid)
            config["vocabulary"].update(vocab)

        return config


def write_ctd_ncfile(
    odf_path,
    output_path,
    config=None,
    variable_header_section="PARAMETER_HEADER",
    original_prefix_var_attribute="original_",
    variable_name_attribute="CODE",
):

    if not os.path.isdir(config["TEST_FILES_OUTPUT"]):
        os.mkdir(config["TEST_FILES_OUTPUT"])

    print(os.path.split(odf_path)[-1])
    # Parse the ODF file with the CIOOS python parsing tool
    metadata, raw_data = odf.read(odf_path)

    # create unique ID for each profile
    # TODO This was in the previous code, not sure if we want to handle it that way. Different groups may have
    #  their own convention. A file Name variable is at least generated for now.
    # profile_id = f"{metadata['cruiseNumber']}-{metadata['eventNumber']}-{metadata['eventQualifier']}"

    # Let's convert to an xarray dataset
    ds = raw_data.to_xarray()

    # Write global attributes
    ds.attrs.update(config["global_attributes"])  # From the config file
    ds = odf.global_attributes_from_header(ds, metadata)  # From ODF header

    # Add New Variables
    ds = odf.generate_variables_from_header(ds, metadata, config['cdm_data_type'])  # From ODF header
    # geographic_area
    ds["geographic_area"] = get_geo_code([ds["longitude"].mean(), ds["latitude"].mean()], config["polygons_dict"])

    # Add file name as a variable
    ds['file_id'] = os.path.split(odf_path)[-1]  # original file name
    if config.get('cdm_data_type') == 'Profile':
        ds['profile_id'] = os.path.split(odf_path)[-1]
    elif config.get('cdm_data_type') == 'TimeSeries':
        ds['timeseries_id'] = os.path.split(odf_path)[-1]
    elif config.get('cdm_data_type') == 'Trajectory':
        ds['trajectory_id'] = os.path.split(odf_path)[-1]

    # Add Variable Attributes
    # Convert metadata variable attributes list to dictionary
    var_attributes = {
        var[variable_name_attribute]: {
            original_prefix_var_attribute + att: value
            for att, value in var.items()
        }
        for var in metadata[variable_header_section]
    }

    # Add Vocabulary attributes
    var_attributes = odf.define_odf_variable_attributes(
        var_attributes,
        organizations=config["organisationVocabulary"],
        vocabulary=config["vocabulary"],
    )

    # Add variable attributes to ds variables
    for var, attrs in var_attributes.items():
        ds[var].attrs.update(attrs)

        # Keep the original long_name and units for now, except if it doesn't exist or
        # None or was populated already (flags)
        if "long_name" not in ds[var].attrs and ds[var].attrs.get(
            "original_NAME"
        ):
            ds[var].attrs["long_name"] = ds[var].attrs.get("original_NAME")
        if (
            "units" not in ds[var].attrs
            and ds[var].attrs.get("original_UNITS")
            and ds[var].attrs["original_UNITS"]
            not in ["nan", "(none)", "none"]
        ):
            ds[var].attrs["units"] = ds[var].attrs.get("original_UNITS")
    ds = xarray_methods.add_variable_attributes(ds)

    # Generate extra variables (BODC, Derived)
    # TODO This need to be added in the near future to make the data output fully usable

    # Add geospatial and geometry related global attributes
    ds = xarray_methods.get_spatial_coverage_attributes(ds)  # Just add spatial/time range as attributes
    ds = xarray_methods.derive_cdm_data_type(ds, config['cdm_data_type'])  # Retrieve geometry information
    ds = xarray_methods.convert_variables_to_erddap_format(ds)  # Add encoding information to xarray
    ds = xarray_methods.define_index_dimensions(ds)  # Assign the right dimension

    # Finally save the xarray dataset to a NetCDF file!!!
    ds.to_netcdf(output_path)


def convert_test_files(config):
    flist = glob.glob(config["TEST_FILES_PATH"] + "/*.ODF")

    if not os.path.isdir(config["TEST_FILES_OUTPUT"]):
        os.mkdir(config["TEST_FILES_OUTPUT"])

    for f in flist:
        try:
            print(f)
            write_ctd_ncfile(
                odf_path=f,
                output_path=config["TEST_FILES_OUTPUT"]
                + "{}.nc".format(os.path.basename(f)),
                config=config,
            )

        except Exception as e:
            # Copy problematic files to a subfolder
            if not os.path.isdir(os.path.join(os.path.dirname(f), "failed")):
                os.mkdir(os.path.join(os.path.dirname(f), "failed"))
            shutil.copy(
                f,
                os.path.join(
                    os.path.dirname(f), "failed", os.path.split(f)[-1]
                ),
            )

            print("***** ERROR***", f)
            print(e)
            print(traceback.print_exc())


def read_geojson_file_list(fileList):
    # read geojson files
    polygons_dict = {}
    for fname in fileList:
        polygons_dict.update(read_geojson(fname))
    return polygons_dict


#
# make this file importable
#
if __name__ == "__main__":
    CONFIG_PATH = fix_path("./config.json")
    TEST_FILES_PATH = fix_path("./test/test_files/")
    TEST_FILES_OUTPUT = fix_path("./test/temp_noOce/")

    config = read_config(CONFIG_PATH)
    config.update(
        {"polygons_dict": read_geojson_file_list(config["geojsonFileList"])}
    )
    config.update(
        {
            "TEST_FILES_PATH": TEST_FILES_PATH,
            "TEST_FILES_OUTPUT": TEST_FILES_OUTPUT,
        }
    )

    convert_test_files(config)
