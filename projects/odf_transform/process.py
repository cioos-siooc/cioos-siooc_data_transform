"""General module use to convert ODF files into a NetCDF CF, ACDD compliant format."""
import json
import logging
import os
import re

import numpy as np
import pandas as pd

from odf_transform import attributes
from odf_transform import parser as odf_parser

import cioos_data_transform.parse.seabird as seabird
from cioos_data_transform.utils.utils import (
    get_geo_code,
    get_nearest_station,
    read_geojson,
)
from cioos_data_transform.utils.xarray_methods import standardize_dataset

from ._version import __version__

logger = logging.getLogger(__name__)

# Adapt Logger to have incorporated the odf_file name
logger = logging.LoggerAdapter(logger, {"odf_file": None})
seabird.logger = logging.LoggerAdapter(seabird.logger, {"odf_file": None})
attributes.logger = logging.LoggerAdapter(attributes.logger, {"odf_file": None})
odf_parser.logger = logging.LoggerAdapter(odf_parser.logger, {"odf_file": None})


MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(MODULE_PATH, "config.json")
ODF_TRANSFORM_MODULE_PATH = MODULE_PATH
reference_stations = pd.read_csv(os.path.join(MODULE_PATH, "reference_stations.csv"))
reference_stations_position_list = reference_stations[
    ["station", "latitude", "longitude"]
].to_records(index=False)

MAXIMUM_DISTANCE_NEAREST_STATION_MATCH = 3  # km


def eval_config_input(text_to_eval):
    """
    Evaluate an fstring expression without using the eval
    function and only relying on local and global variables.
    """
    output = text_to_eval
    items = re.findall(r"(\{(.*)\})", text_to_eval)
    for expression, variable in items:
        if variable in locals():
            output = output.replace(expression, locals()[variable])
        elif variable in globals():
            output = output.replace(expression, globals()[variable])
        else:
            raise RuntimeError(f"Failed to eval {expression}")
    return output


def read_config(config_file):
    """Load configuration json file and vocabulary file."""
    # read json file with information on dataset etc.
    with open(config_file, encoding="UTF-8") as fid:
        config = json.load(fid)

    # Apply fstring to geojson paths
    config["geographic_areas"] = read_geojson_file_list(
        [eval_config_input(fpath) for fpath in config["geojsonFileList"]]
    )

    # Read Vocabulary file
    vocab = pd.read_csv(
        eval_config_input(config["vocabularyFile"]), index_col=["Vocabulary", "name"]
    )
    config["vocabulary"] = vocab.fillna(np.nan).replace({np.nan: None})
    return config


def read_geojson_file_list(file_list):
    """Read geojson files"""
    polygons_dict = {}
    for fname in file_list:
        polygons_dict.update(read_geojson(fname))
    return polygons_dict


def write_ctd_ncfile(odf_path, config=None):
    """Convert odf files to a CIOOS/ERDDAP compliant NetCDF format"""
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
            "ODF_transform is not yet compatible with ODF Data Type: %s",
            metadata["EVENT_HEADER"]["DATA_TYPE"],
        )
        return

    # Convert to an xarray dataset
    dataset = raw_data.to_xarray()

    # Write global and variable attributes
    dataset.attrs = config["global_attributes"]
    dataset = attributes.global_attributes_from_header(dataset, metadata)
    dataset.attrs[
        "history"
    ] += f"# Convert ODF to NetCDF with cioos_data_trasform.odf_transform V {__version__}\n"
    for var, attrs in metadata["variable_attributes"].items():
        if var in dataset:
            dataset[var].attrs = attrs

    # Handle ODF flag variables
    dataset = odf_parser.odf_flag_variables(dataset, config.get("flag_convention"))

    # Define coordinates variables from attributes, assign geographic_area and nearest stations
    dataset = attributes.retrieve_coordinates(dataset)
    dataset.attrs["geographic_area"] = get_geo_code(
        [dataset["longitude"].mean(), dataset["latitude"].mean()],
        config["geographic_areas"],
    )

    nearest_station = get_nearest_station(
        reference_stations_position_list,
        (dataset["latitude"], dataset["longitude"]),
        MAXIMUM_DISTANCE_NEAREST_STATION_MATCH,
    )
    if nearest_station:
        dataset.attrs["station"] = nearest_station
    elif (
        dataset.attrs.get("station")
        and dataset.attrs.get("station") not in reference_stations["station"].tolist()
        and re.match(r"[^0-9]", dataset.attrs["station"])
    ):
        logger.warning(
            "Station %s [%sN, %sE] is missing from the reference_station.",
            dataset.attrs["station"],
            dataset["latitude"].mean().values,
            dataset["longitude"].mean().values,
        )

    # Add Vocabulary attributes
    dataset = odf_parser.get_vocabulary_attributes(
        dataset,
        organizations=config["organisationVocabulary"],
        vocabulary=config["vocabulary"],
    )

    # Fix flag variables with some issues to map
    dataset = odf_parser.fix_flag_variables(dataset)

    # Instrument specific variables and attributes
    if dataset.attrs["instrument_manufacturer_header"].startswith("* Sea-Bird"):
        dataset = seabird.add_seabird_instruments(
            dataset,
            dataset.attrs["instrument_manufacturer_header"],
            match_by="sdn_parameter_urn",
        )
        dataset = seabird.update_attributes_from_seabird_header(
            dataset, dataset.attrs["instrument_manufacturer_header"]
        )

    # Add geospatial and geometry related global attributes
    # Just add spatial/time range as attributes
    initial_attrs = dataset.attrs.keys()
    dataset = standardize_dataset(dataset, utc=True)
    dropped_attrs = [var for var in initial_attrs if var not in dataset.attrs]
    if dropped_attrs:
        logger.info(f"Drop empty attributes: {dropped_attrs}")

    # Handle coordinates and dimensions
    coords = [
        coord
        for coord in ["time", "latitude", "longitude", "depth"]
        if coord in dataset
    ]
    dataset = dataset.set_coords(coords)
    if (
        dataset.attrs["cdm_data_type"] == "Profile"
        and "index" in dataset
        and "depth" in dataset
    ):
        dataset = dataset.swap_dims({"index": "depth"}).drop_vars("index")

    # Log variables available per file
    logger.info(f"Variable List: {list(dataset)}")

    # Save dataset to a NetCDF file
    if config["output_path"] is None:
        output_path = odf_path + ".nc"
    else:
        output_path = os.path.join(
            eval_config_input(output_path), os.path.basename(odf_path) + ".nc"
        )

    # Add file suffix if present within the config
    if config.get("addFileNameSuffix"):
        output_path = re.sub(r"\.nc$", config["addFileNameSuffix"] + ".nc", output_path)

    # Review if output path folders exists if not create them
    dirname = os.path.dirname(output_path)
    if not os.path.isdir(dirname):
        logger.info(f"Generate output directory: {output_path}")
        os.makedirs(dirname)

    dataset.to_netcdf(output_path)


def convert_odf_file(file, config: dict = None):
    """Method to convert odf file with a tuple input that expect the format
    (file, config)"""
    # Handle default inputs
    if config is None:
        config = read_config(DEFAULT_CONFIG_PATH)

    logger.extra["odf_file"] = os.path.basename(file)
    try:
        write_ctd_ncfile(
            odf_path=file,
            config=config,
        )
    except Exception:
        logger.error(f"Failed to convert: {file}", exc_info=True)
