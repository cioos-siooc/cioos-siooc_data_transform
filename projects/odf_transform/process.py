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


def read_config(config_file):
    """Load configuration json file and vocabulary file."""
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
    return config


def write_ctd_ncfile(odf_path, config=None):
    """Convert odf files to a CIOOS/ERDDAP compliant NetCDF format"""
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


def convert_odf_file(file, config: dict = None):
    """Method to convert odf file with a tuple input that expect the format
    (file, config)"""
    # Handle default inputs
    if config is None:
        config = read_config(DEFAULT_CONFIG_PATH)

    # Update submodule LoggerAdapter to include the odf_path
    log = {"file": file}
    seabird.logger.extra.update(log)
    attributes.logger.extra.update(log)
    odf_parser.logger.extra.update(log)
    logger.extra.update(log)

    logger.extra["file"] = os.path.basename(file)
    try:

        write_ctd_ncfile(
            odf_path=file,
            config=config,
        )
    except Exception:
        logger.error(f"Failed to convert: {file}", exc_info=True)


def input_from_program_logs(program_log_path, files, global_config):
    """Generate input based on the program logs available

    input_from_program_logs compile all the different program logs available within the directory
    and match each files that neeeds a conversion to the appropriate mission. For each mission,
    it updates the configuration global attributes to include any extra attributes available
    within the log.

    A list of inputs is then generated which can be run by the ODF conversion tool..

    Args:
        program_log_path (string):
        files (list): List of files
        polygons (dict): dictionary of geojson regions
        output_path (str): path to output files to
        config (dict)): [description]

    Returns:
        list: list of inputs to run the conversion for each files and and their
        specific mission attributes.
    """

    def generate_mission_config(mission_attrs):
        """Generate mission specific configuration"""
        mission_config = global_config.copy()
        mission_config["global_attributes"] = {
            **mission_config["global_attributes"],
            **mission_attrs.dropna().drop(["mission", "mission_file_list"]).to_dict(),
        }
        return mission_config

    # Load the different logs and map the list of files available to the programs
    program_logs = [
        item
        for item in os.listdir(program_log_path)
        if os.path.isfile(os.path.join(program_log_path, item))
    ]

    df_logs = pd.DataFrame()
    logger.info(f"Load Program Logs: {program_logs}")
    for log in program_logs:
        # Read program logs
        df_log = pd.read_csv(os.path.join(program_log_path, log))

        # Extract the program name from the file name
        if "program" not in df_log:
            df_log["program"] = log.rsplit(".", 1)[0]

        # Append to previous logs
        df_logs = df_logs.append(df_log, ignore_index=True)

    # Review duplicated mission input in log
    if df_logs["mission"].duplicated().any():
        duplicated_mission = df_logs.loc[df_logs["mission"].duplicated()]
        logger.error(f"Duplicated mission inputs were detected: {duplicated_mission}")
        return None

    # Ignore files that the mission isn't listed
    search_regexp = "|".join(df_logs["mission"].tolist())
    files = [file for file in files if re.search(search_regexp, file)]
    if len(files) == 0:
        return None
    logger.info(f"{len(files)} files are associated to a mission.")

    logger.info("Group files available for conversion by missions")
    df_logs["mission_file_list"] = df_logs.progress_apply(
        lambda x: [file for file in files if re.search(x["mission"], file)] or None,
        axis=1,
    )
    # Drop Mission with no files available
    df_logs = df_logs.dropna(subset=["mission_file_list"])

    logger.info(
        f"Generate Mission Specific Configuration for {len(files)} files associated "
        + f"with {len(df_logs)} missions"
    )
    df_logs["mission_config"] = df_logs.progress_apply(generate_mission_config, axis=1)

    # Generate input for each files
    convert_odf_inputs = []
    logger.info("Retrieve mission matching files")
    for _, df_mission in df_logs.iterrows():
        convert_odf_inputs += [
            (mission_file, df_mission["mission_config"])
            for mission_file in df_mission["mission_file_list"]
        ]

    return convert_odf_inputs


def run_odf_conversion(config):

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

    

    if config["overwrite"]:
        # overwrite all files
        logger.info(f"Overwrite all {len(odf_files_list)}")
    elif os.path.isfile(output_path):
        logger.info(f"Overwrite file: {output_path}")
    else:
        # Get directory where all the files should be outputted
        search_output_path = input_path if output_path is None else output_path

        # Get files available in output_path and their last edit time
        search_output_path_files = glob(
            f"{search_output_path}/**/{config['fileNameRegex']}{config.get('addFileNameSuffix','')}.nc",
            recursive=True,
        )

        outputted_files = {
            os.path.basename(file): {
                "path": file,
                "last_modified": os.path.getmtime(file),
            }
            for file in search_output_path_files
        }

        # Output new file if not netcdf equivalent exist or netcdf is older than odf
        overwrite_list = []
        new_list = []
        for file in odf_files_list:
            filename = os.path.basename(file)
            ncfile = f"{filename + config['addFileNameSuffix']}.nc"

            if ncfile not in outputted_files:
                new_list.append(file)
            elif outputted_files[ncfile]["last_modified"] < os.path.getmtime(file):
                # source file is more recent than the corresponding netcdf
                overwrite_list.append(file)
        logger.info(
            f"Create {len(new_list)}/{len(odf_files_list)} files - "
            + f"Overwrite {len(overwrite_list)}/{len(odf_files_list)} files"
        )
        odf_files_list = overwrite_list
        odf_files_list += new_list

    # Generate inputs
    if config["program_logs_path"]:
        inputs = input_from_program_logs(
            config["program_logs_path"],
            odf_files_list,
            config,
        )
    else:
        inputs = [(file, config) for file in odf_files_list]

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
    if len(inputs) > 100:
        logger.info(
            f"Run ODF conversion in multiprocessing with {config['n_workers']} workers"
        )
        with Pool(config["n_workers"]) as pool:
            list(tqdm(pool.imap(convert_odf_file, inputs), **tqdm_dict))
    elif 0 < len(inputs) < 100:
        print("Run ODF Conversion")
        for item in tqdm(inputs, **tqdm_dict):
            convert_odf_file(*item)
