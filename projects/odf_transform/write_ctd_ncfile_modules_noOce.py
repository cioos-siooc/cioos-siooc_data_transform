import glob
import json
import os
import traceback
import re

import xarray as xr
import cioos_data_transform.utils.odf as odf
import cioos_data_transform.utils.xarray as xarray_methods
import cioos_data_transform.utils.erddap_netcdf as erddap_netcdf
from cioos_data_transform.utils.utils import fix_path
from cioos_data_transform.utils.utils import get_geo_code, read_geojson
import datetime as dt
import pandas as pd

def read_config(config_file):
    # read json file with information on dataset etc.
    with open(config_file) as fid:
        config = json.load(fid)

        # Read Vocabulary file
        for vocab_file in config['vocabularyFileList']:
            config.update({"vocabulary": {}})
            with open(vocab_file) as fid:
                vocab = json.load(fid)
            config["vocabulary"].update(vocab)

        return config


def write_ctd_ncfile(odf_path,
                     output_path,
                     config=None,
                     variable_header_section='PARAMETER_HEADER',
                     original_prefix_var_attribute='original_',
                     variable_name_attribute='CODE'):


    if not os.path.isdir(TEST_FILES_OUTPUT):
        os.mkdir(TEST_FILES_OUTPUT)

    print(os.path.split(odf_path)[-1])
    # Parse the ODF file with the CIOOS python parsing tool
    metadata, raw_data = odf.read(odf_path)

    # Add the file name as variable
    raw_data['original_file'] = os.path.split(odf_path)[-1]
    raw_data = raw_data.set_index('original_file')
    # create unique ID for each profile
    #profile_id = f"{metadata['cruiseNumber']}-{metadata['eventNumber']}-{metadata['eventQualifier']}"

    # Let's convert to an xarray dataset.
    ds = raw_data.to_xarray()

    # #### Write global attributes ####
    # From the config file
    ds.attrs.update(config['global_attributes'])

    # # From ODF header
    ds = xarray_methods.add_variables_from_dict(ds,
                                                config['global_attributes'],
                                                None,
                                                metadata,
                                                global_attribute=True)
    ds.attrs['header'] = json.dumps(metadata, ensure_ascii=False, indent=False)

    # Add Variable from Global Attributes
    if 'variables_from_header' in config:
        ds = xarray_methods.add_variables_from_dict(ds, config['variables_from_header'],
                                                    'original_file', dictionary=metadata)
        variables_order = config['variables_from_header'].keys()

    # Add Variable Attributes
    # Convert metadata variable attributes list to dictionary
    var_attributes = {var[variable_name_attribute]: {original_prefix_var_attribute + att: value
                                                     for att, value in var.items()}
                      for var in metadata[variable_header_section]}

    # Add Vocabulary attributes
    var_attributes = odf.define_odf_variable_attributes(var_attributes,
                                                        organizations=config['organisationVocabulary'],
                                                        vocabulary=config['vocabulary'])

    # Add variable attributes to ds variables
    for var, attrs in var_attributes.items():
        ds[var].attrs.update(attrs)

        # Keep the original long_name and units for now
        ds[var].attrs['long_name'] = ds[var].attrs.get('original_NAME')
        ds[var].attrs['units'] = ds[var].attrs.get('original_UNITS')

    # Generate Extra Variables

    # Add Geospatial and Geometry related attributes
    ds = erddap_netcdf.get_spatial_coverage_attributes(ds)
    ds = erddap_netcdf.convert_variables_to_erddap_format(ds)
    # global_attrs["id"] = profile_id
    # global_attrs["cdm_profile_variables"] = "time"
    # global_attrs["featureType"] = "profile"


    # Finally save the xarray dataset to a NetCDF file!!!
    variable_order = []
    ds.to_netcdf(output_path + "{}.nc".format(os.path.basename(odf_path)))


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


