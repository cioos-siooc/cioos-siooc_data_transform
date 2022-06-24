import glob
import json
import os
import re

import numpy as np

from odf_transform import parser as odf_parser
from odf_transform import attributes
from odf_transform.__main__ import __version__

from cioos_data_transform.utils.xarray_methods import standardize_dataset
import cioos_data_transform.parse.seabird as seabird
from cioos_data_transform.utils.utils import get_geo_code, read_geojson, get_nearest_station
import pandas as pd

import logging

logger = logging.getLogger(__name__)

# Adapt Logger to have incorporated the odf_file name
logger = logging.LoggerAdapter(logger, {"odf_file": None})
seabird.logger = logging.LoggerAdapter(seabird.logger, {"odf_file": None})
attributes.logger = logging.LoggerAdapter(attributes.logger, {"odf_file": None})
odf_parser.logger = logging.LoggerAdapter(odf_parser.logger, {"odf_file": None})


MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(MODULE_PATH, "config.json")
ODF_TRANSFORM_MODULE_PATH = MODULE_PATH
reference_stations = pd.read_csv(os.path.join(MODULE_PATH,"reference_stations.csv"))
reference_stations_position_list = reference_stations[['station','latitude','longitude']].to_records(index=False)


def read_config(config_file):
    """Function to load configuration json file and vocabulary file."""
    # read json file with information on dataset etc.
    with open(config_file, encoding="UTF-8") as fid:
        config = json.load(fid)

    # Apply fstring to geojson paths
    config["geojsonFileList"] = [eval(f"f'{fpath}'") for fpath in config["geojsonFileList"]]

    # Read Vocabulary file
    vocab = pd.read_csv(eval(f"f'{config['vocabularyFile']}'"), index_col=["Vocabulary", "name"])
    config["vocabulary"] = vocab.fillna(np.nan).replace({np.nan: None})
    return config


def read_geojson_file_list(file_list):
    """Read geojson files"""
    polygons_dict = {}
    for fname in file_list:
        polygons_dict.update(read_geojson(fname))
    return polygons_dict


def write_ctd_ncfile(odf_path, output_path=None, config=None, polygons = None):
    """Method use to convert odf files to a CIOOS/ERDDAP compliant NetCDF format"""
    if polygons is None:
        polygons = {}

    # Update submodule LoggerAdapter to include the odf_path 
    log = {"odf_path": odf_path}
    seabird.logger.extra.update(log)
    attributes.logger.extra.update(log)
    odf_parser.logger.extra.update(log)
    logger.extra.update(log)

    # Parse the ODF file with the CIOOS python parsing tool
    metadata, raw_data = odf_parser.read(odf_path)

    # Review ODF data type compatible with odf_transform
    if metadata["EVENT_HEADER"]["DATA_TYPE"] not in ["CTD", "BT", "BOTL"]:
        logger.warning(
            f'ODF_transform is not yet compatible with ODF Data Type: {metadata["EVENT_HEADER"]["DATA_TYPE"]}'
        )
        return

    # Convert to an xarray dataset
    ds = raw_data.to_xarray()

    # Write global and variable attributes
    ds.attrs = config["global_attributes"]
    ds = attributes.global_attributes_from_header(ds, metadata)
    ds.attrs[
        "history"
    ] += f"# Convert ODF to NetCDF with cioos_data_trasform.odf_transform V {__version__}"
    for var, attrs in metadata["variable_attributes"].items():
        if var in ds:
            ds[var].attrs = attrs

    # Handle ODF flag variables
    ds = odf_parser.odf_flag_variables(ds, config.get("flag_convention"))

    # Define coordinates variables from attributes, assign geographic_area and nearest stations
    ds = attributes.retrieve_coordinates(ds)
    ds.attrs["geographic_area"] = get_geo_code(
        [ds["longitude"].mean(), ds["latitude"].mean()], polygons
    )
    MAXIMUM_DISTANCE_NEAREST_STATION_MATCH = 3 #km
    nearest_station = get_nearest_station(reference_stations_position_list,(ds['latitude'],ds['longitude']),MAXIMUM_DISTANCE_NEAREST_STATION_MATCH)
    if nearest_station:
        ds.attrs["station"] = nearest_station
    elif ds.attrs.get('station') and ds.attrs.get('station') not in reference_stations['station'].tolist() and re.match("[^0-9]",ds.attrs['station']):
        logger.warning(f"Station {ds.attrs['station']} [{ds['latitude'].mean().values}N, {ds['longitude'].mean().values}E] is missing from the reference_station.")

    # Add Vocabulary attributes
    ds = odf_parser.get_vocabulary_attributes(
        ds,
        organizations=config["organisationVocabulary"],
        vocabulary=config["vocabulary"],
    )

    # Fix flag variables with some issues to map
    ds = odf_parser.fix_flag_variables(ds)

    # Instrument specific variables and attributes
    if ds.attrs["instrument_manufacturer_header"].startswith("* Sea-Bird"):
        ds = seabird.add_seabird_instruments(
            ds, ds.attrs["instrument_manufacturer_header"], match_by="sdn_parameter_urn"
        )
        ds = seabird.update_attributes_from_seabird_header(
            ds, ds.attrs["instrument_manufacturer_header"]
        )

    # Add geospatial and geometry related global attributes
    # Just add spatial/time range as attributes
    initial_attrs = ds.attrs.keys()
    ds = standardize_dataset(ds, utc=True)
    dropped_attrs = [var for var in initial_attrs if var not in ds.attrs]
    if dropped_attrs:
        logger.info(f"Drop empty attributes: {dropped_attrs}")

    # Handle coordinates and dimensions
    coords = [coord for coord in ['time','latitude','longitude','depth'] if coord in ds]
    ds = ds.set_coords(coords)
    if ds.attrs["cdm_data_type"] == "Profile" and "index" in ds and "depth" in ds:
        ds = ds.swap_dims({"index": "depth"}).drop_vars("index")

    # Log variables available per file
    logger.info(f"Variable List: {list(ds)}")

    # Save dataset to a NetCDF file
    if output_path is None:
        output_path = odf_path + ".nc"
    if re.search("\{|\}", output_path):
        # if output_path is f-string, evaluate the output_path
        output_path = os.path.join(eval(f'f"{output_path}"'),os.path.basename(odf_path) + '.nc')

    # Add file suffix if present within the config
    if config.get("addFileNameSuffix"):
        output_path = re.sub("\.nc$", config["addFileNameSuffix"] + ".nc", output_path)

    # Review if output path folders exists if not create them
    dirname = os.path.dirname(output_path)
    if not os.path.isdir(dirname):
        logger.info(f'Generate output directory: {output_path}')
        os.makedirs(dirname)

    ds.to_netcdf(output_path)


# Load Default
polygons = read_geojson_file_list(glob.glob(MODULE_PATH + "/geojson_files/*.geojson"))
config = read_config(DEFAULT_CONFIG_PATH)


def convert_odf_file(input, polygons=polygons, output_path=None, config=config):
    """Method to convert odf file with a tuple input that expect the format (file, polygons, output_path, config)"""
    if type(input) == tuple:
        file, polygons, output_path, config = input
    else:
        file = input
        
    logger.extra["odf_file"] = os.path.basename(file)
    try:
        write_ctd_ncfile(
            odf_path=file, polygons=polygons, output_path=output_path, config=config,
        )
    except Exception as e:
        logger.error(f"Failed to convert: {file}", exc_info=True)
