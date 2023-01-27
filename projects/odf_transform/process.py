"""General module use to convert ODF files into a NetCDF CF, ACDD compliant format."""
import copy
import json
import logging
import os
import re
from glob import glob
from multiprocessing import Pool

import numpy as np
import pandas as pd
from cioos_data_transform.utils.utils import read_geojson
from odf_transform import attributes
from odf_transform import parser as odf_parser
from odf_transform._version import __version__
from odf_transform.utils import seabird
from odf_transform.utils.standarize_attributes import standardize_dataset
from tqdm import tqdm

tqdm.pandas()

no_file_logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(no_file_logger, {"file": None})


MODULE_PATH = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG_PATH = os.path.join(MODULE_PATH, "config.json")
ODF_TRANSFORM_MODULE_PATH = MODULE_PATH


def read_config(config_file: str = DEFAULT_CONFIG_PATH) -> dict:
    """Parse json configuration file used to convert ODF files to netcdf.

    Args:
        config_file (str, optional): Path to json configuration file.
            Defaults to DEFAULT_CONFIG_PATH.

    Returns:
        dict: parsed configuration
    """
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

    # Reference Platforms
    # convert a dictionary with lowered platform names
    df_platforms = pd.concat(
        pd.read_csv(
            os.path.join(file),
            dtype={"wmo_platform_code": "string"},
        )
        for file in config["reference_platforms_files"]
    )
    config["reference_platforms"] = {
        index.lower(): row.dropna().to_dict()
        for index, row in df_platforms.set_index("platform_name", drop=False).iterrows()
    }

    # Read Vocabulary file
    vocab = pd.read_csv(config["vocabularyFile"], index_col=["Vocabulary", "name"])
    config["vocabulary"] = vocab.fillna(np.nan).replace({np.nan: None})

    # Read program logs
    if config["program_log_path"]:
        program_logs = []
        for file in glob(config["program_log_path"] + "*.csv"):
            df_temp = pd.read_csv(file, dtype={"mission": str})
            df_temp.insert(0, "program", os.path.basename(file)[:-4])
            program_logs += [df_temp]
        config["program_log"] = pd.concat(program_logs)
    else:
        config["program_log"] = None

    # Attribute mapping corrections
    config["attribute_mapping_corrections"] = {}
    for file in config["attribute_mapping_corrections_files"]:
        with open(file, encoding="utf-8") as f:
            config["attribute_mapping_corrections"].update(json.load(f))

    # File specific corrections
    if config["file_specific_attributes_path"]:
        with open(config["file_specific_attributes_path"], encoding="UTF-8") as f:
            config["file_specific_attributes"] = json.load(f)
    else:
        config["file_specific_attributes"] = None

    return config


def odf_to_netcdf(odf_path, config=None):
    """Convert an ODF file to a netcdf.
    Args:
        odf_path (str): path to ODF file to convert
        config (dictionary, optional): Conversion configuration to apply.
            Defaults to odf_transform/config.json.
    """

    # Use default config if no config is given
    if config is None:
        config = read_config(DEFAULT_CONFIG_PATH)

    # Parse the ODF file with the CIOOS python parsing tool
    metadata, raw_data = odf_parser.read(odf_path)
    dataset = raw_data.to_xarray()

    # Review ODF data type compatible with odf_transform
    if metadata["EVENT_HEADER"]["DATA_TYPE"] not in ["CTD", "BT", "BOTL"]:
        logger.warning(
            "ODF_transform is not yet compatible with ODF Data Type: %s",
            metadata["EVENT_HEADER"]["DATA_TYPE"],
        )
        return

    # Write global and variable attributes
    dataset.attrs = config["global_attributes"]
    dataset.attrs["source"] = odf_path
    dataset = attributes.global_attributes_from_header(dataset, metadata, config=config)
    dataset.attrs[
        "history"
    ] += f"# Convert ODF to NetCDF with cioos_data_trasform.odf_transform V {__version__}\n"
    for var, attrs in metadata["variable_attributes"].items():
        if var in dataset:
            dataset[var].attrs = attrs

    # Handle ODF flag variables
    dataset = odf_parser.odf_flag_variables(dataset, config.get("flag_convention"))

    # Define coordinates variables from attributes, assign geographic_area and nearest stations
    dataset = attributes.generate_coordinates_variables(dataset)
    dataset = attributes.generate_spatial_attributes(dataset, config)

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
    coordinates = ["time", "latitude", "longitude", "depth"]
    dataset = dataset.set_coords([var for var in coordinates if var in dataset])
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
        output_path = odf_path + config["addFileNameSuffix"] + ".nc"
    else:
        # Retrieve subfolder path
        subfolders = [
            str(dataset.attrs.get(
                key,
                dataset["time"].min().dt.year.item(0)
                if default == "year"
                else default,
            ))
            for key, default in config["subfolder_attribute_output_path"].items()
            if dataset.attrs.get(key, default)
        ]
        output_path = os.path.join(
            eval('f"{}"'.format(config["output_path"])),
            os.path.basename(odf_path) + config["addFileNameSuffix"] + ".nc",
        )

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
        logger.error("Conversion failed!!!", exc_info=True)


def run_odf_conversion_from_config(config):
    """Run ODF conversion from configuration file

    Args:
        config (dict, str): Configuration dictionary or path to json file
        to apply conversion.
    """

    def _remove_already_converted_files(input_files, config):
        """Review file list to convert and drop any files that already
        exist in output directory and that last edit time exceed the original file.

        Args:
            input_files (list): List of files to convert to netcdf format.
            config (dict): Configuration used to apply netcdf conversion

        Returns:
            list: Reduced list of files that needs conversion
        """
        # Get list or already outputted files available in output_path and their last edit time
        output_search_path = (
            config["fileDir"]
            if output_path is None
            else os.path.join(output_path, "**", "*")
        )
        search_output_path_files = glob(
            f"{output_search_path}.ODF{config['addFileNameSuffix']}.nc",
            recursive=True,
        )
        outputted_files = {
            os.path.basename(file.replace(config["addFileNameSuffix"] + ".nc", "")): {
                "path": file,
                "last_modified": os.path.getmtime(file),
            }
            for file in search_output_path_files
        }
        return [
            file
            for file in input_files
            if os.path.basename(file) not in outputted_files
            or outputted_files[os.path.basename(file)]["last_modified"]
            < os.path.getmtime(file)
        ]

    def _generate_input_by_program(files, config):
        """Generate mission specific input to apply for the conversion

        Args:
            files (list): List of files to apply the conversion to
            config (dict): General configuration to be used for the conversion

        Returns:
            list: list of inputs used for each files [(file_path, file_specific_configuration),...]
        """
        logger.info(
            "Generate Mission Specific Configuration for %s files associated with %s missions",
            len(files),
            len(config["program_log"]),
        )
        inputs = []
        tqdm_dict = {
            "desc": "Generate mission specific configuration",
            "total": len(config["program_log"]),
        }
        for mission, row in tqdm(
            config["program_log"].set_index("mission", drop=False).iterrows(),
            **tqdm_dict,
        ):
            related_files = [
                file for file in odf_files_list if re.search(mission, file)
            ]
            if related_files:
                mission_config = copy.deepcopy(config)
                mission_config["global_attributes"].update(dict(row.dropna()))
                inputs += [(file, mission_config) for file in related_files]
        return inputs

    # Parse config file if file is given
    if isinstance(config, str):
        config = read_config(config)

    # Handle Input FIles
    logger.info("Retrieve files to process")
    odf_files_list = glob(
        config["fileDir"],
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

    if config["program_log"] is not None:
        # Consider only files related to missions available in the program_log
        missions = config["program_log"]["mission"].values
        # Review first if any files are matching a specific mission
        files_list = ", ".join(odf_files_list)
        for _, row in config["program_log"].iterrows():
            if re.search(row["mission"], files_list) is None:
                logger.warning(
                    "No file available is related to program_log input %s",
                    row.dropna().to_dict(),
                )
        unmatched_odfs = [
            file for file in odf_files_list if not re.search("|".join(missions), file)
        ]
        odf_files_list = [
            file for file in odf_files_list if re.search("|".join(missions), file)
        ]
        if unmatched_odfs:
            logger.warning(
                "%s odf files aren't matched to any provided missions",
                len(unmatched_odfs),
            )
            with open("unmatched_odfs.txt", "w", encoding="UTF-8") as file_handle:
                file_handle.write("\n".join(unmatched_odfs))
        else:
            logger.warning("All odf files available match a mission")

    # Sort files that needs to be converted
    if config["overwrite"]:
        # overwrite all files
        logger.info(f"Overwrite all {len(odf_files_list)}")
    elif os.path.isfile(output_path):
        logger.info(f"Overwrite file: {output_path}")
    else:
        odf_files_list = _remove_already_converted_files(odf_files_list, config)

    # Generate inputs
    if config["program_log"] is not None:
        inputs = _generate_input_by_program(odf_files_list, config)
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
    else:
        print("Run ODF Conversion")
        for odf_convert_input in tqdm(inputs, **tqdm_dict):
            odf_to_netcdf_with_log(odf_convert_input)
