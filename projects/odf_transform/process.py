"""General module use to convert ODF files into a NetCDF CF, ACDD compliant format."""
import json
import logging
import os
import re
from glob import glob
from multiprocessing import Pool

import numpy as np
import pandas as pd
from tqdm import tqdm

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

tqdm.pandas()

no_file_logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(no_file_logger, {"file": None})


MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(MODULE_PATH, "config.json")
ODF_TRANSFORM_MODULE_PATH = MODULE_PATH


def read_config(config_file: str = DEFAULT_CONFIG_PATH) -> dict: 
    # read json file with information on dataset etc.
    with open(config_file, encoding="UTF-8") as fid:
        json_text = fid.read()
    # Replace any reference to {ODF_TRANSFORM_MODULE_PATH} by module path
    json_text = json_text.replace(
        "{ODF_TRANSFORM_MODULE_PATH}", ODF_TRANSFORM_MODULE_PATH
    )
    config = json.loads(json_text)

    # Apply fstring to geojson paths
    config["geographic_areas"] = {}
    for file in config["geographic_area_reference_files"]:
        config["geographic_areas"].update(read_geojson(file))

    # Integrate station lists from geojson files
    config["reference_stations"] = pd.concat(
        [
            pd.json_normalize(
                json.load(open(file, encoding="UTF-8"))["features"], max_level=1
            )
            for file in config["reference_stations_reference_files"]
        ]
    )
    config["reference_stations"].columns = [
        re.sub(r"properties\.|geometry\.", "", col)
        for col in config["reference_stations"]
    ]

    # Read Vocabulary file
    vocab = pd.read_csv(config["vocabularyFile"], index_col=["Vocabulary", "name"])
    config["vocabulary"] = vocab.fillna(np.nan).replace({np.nan: None})
    
    # Read program logs
    if config['program_log_path']:
        program_logs = []
        for file in glob(config['program_log_path']+ '*.csv'):
            df_temp = pd.read_csv(file)
            df_temp.insert(0,'program',os.path.basename(file))
            program_logs += [df_temp]
        config['program_log'] = pd.concat(program_logs)
    else:
        config['program_log'] = None
    return config


def odf_to_netcdf(odf_path, config=None):
    """Convert odf files to a CIOOS/ERDDAP compliant NetCDF format"""
    
    # Use default config if no config is given
    if config is None:
        config = read_config(DEFAULT_CONFIG_PATH)

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
        config["reference_stations"][["station", "latitude", "longitude"]].values,
        (dataset["latitude"], dataset["longitude"]),
        config["maximum_distance_from_station_km"],
    )
    if nearest_station:
        dataset.attrs["station"] = nearest_station
    elif (
        dataset.attrs.get("station")
        and dataset.attrs.get("station")
        not in config["reference_stations"]["station"].tolist()
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
        # Retrieve subfolder path
        subfolders = [
            dataset.attrs.get(key, default)
            for key, default in config.get(
                "subfolder_attribute_output_path", {}
            ).items()
            if dataset.attrs.get(key, default)
        ]
        output_path = os.path.join(
            config["output_path"], *subfolders, os.path.basename(odf_path) + ".nc"
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


def odf_to_netcdf_with_log(inputs):
    """Method to convert odf file with a tuple input that expect the format
    (file, config)"""

    # Update submodule LoggerAdapter to include the odf_path
    log = {"file": inputs[0]}
    seabird.logger.extra.update(log)
    attributes.logger.extra.update(log)
    odf_parser.logger.extra.update(log)
    logger.extra.update(log)
    try:
        odf_to_netcdf(*inputs)
    except Exception:
        logger.error(f"Conversion failed!!!", exc_info=True)

    
def run_odf_conversion_from_config(config):

    # Parse config file if file is given
    if isinstance(config, str):
        config = read_config(config)

    # Handle Input FIles
    logger.info("Retrieve files to process")
    input_path = config["fileDir"]
    if os.path.isfile(input_path):
        odf_files_list = [input_path]
    else:
        odf_files_list = glob(
            f"{input_path}{'/**' if config['recursive'] else ''}/{config['fileNameRegex']}",
            recursive=config["recursive"],
        )
    # Consider only files with specific expressions
    if config["pathRegex"]:
        odf_files_list = [
            file for file in odf_files_list if re.match(config["pathRegex"], file)
        ]
    logger.info(f"{len(odf_files_list)} ODF files are available")

    # Review keep files that needs an update only
    output_path = config["output_path"]

    if config['program_log'] is not None:
        # Consider only files related to missions available in the program_log
        missions = config['program_log']['mission'].values
        odf_files_list = [file for file in odf_files_list if re.search('|'.join(missions),file)]

    if config["overwrite"]:
        # overwrite all files
        logger.info(f"Overwrite all {len(odf_files_list)}")
    elif os.path.isfile(output_path):
        logger.info(f"Overwrite file: {output_path}")
    else:
        # Get list or already outputted files available in output_path and their last edit time
        search_output_path_files = glob(
            os.path.join(
                *[
                    input_path if output_path is None else output_path,
                    "**",
                    f"{config['fileNameRegex']}{config['addFileNameSuffix']}.nc"
                ]
            ),
            recursive=True,
        )
        outputted_files = {
            os.path.basename(file.replace(config['addFileNameSuffix']+'.nc','')): {
                "path": file,
                "last_modified": os.path.getmtime(file),
            }
            for file in search_output_path_files
        }
        # Drop from list to convert any files that are already available and that 
        # the time assciated with original ODF do not exceed the netcdf equivalent.
        odf_files_list = [file for file in odf_files_list if os.path.basename(file) not in outputted_files or outputted_files[os.path.basename(file)]["last_modified"] < os.path.getmtime(file)]

    # Generate inputs
    if config["program_log"] is not None: 
        logger.info(
            "Generate Mission Specific Configuration for %s files associated with %s missions",
            len(odf_files_list),
            len(config['program_log'])
        )   
        inputs = []
        tqdm_dict = {
            'desc':'Generate mission specific configuration',
            "total": len(config['program_log'])
        }
        for mission, row in tqdm(config['program_log'].set_index('mission').iterrows(), **tqdm_dict):
            related_files = [file for file in odf_files_list if mission in file]
            if related_files:
                mission_config = config.copy()
                mission_config['global_attributes'].update(dict(row.dropna()))
                inputs += [ (file, mission_config) for file in related_files]
            else:
                logger.warning('No file available is related to mission: %s',mission)
    else:
        inputs = [(file, config) for file in odf_files_list]

    # Review input list
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

    # If more than 100 files needs conversion run the conversion on multiple processors
    if len(inputs) > 100:
        logger.info(
            f"Run ODF conversion in multiprocessing with {config['n_workers']} workers"
        )
        with Pool(config["n_workers"]) as pool:
            list(tqdm(pool.imap(odf_to_netcdf_with_log, inputs), **tqdm_dict))
    elif 0 < len(inputs) < 100:
        print("Run ODF Conversion")
        for item in tqdm(inputs, **tqdm_dict):
            odf_to_netcdf_with_log(*item)
