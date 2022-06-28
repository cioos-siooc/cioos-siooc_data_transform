import json
import logging
import os
import re
from datetime import datetime
from difflib import get_close_matches

import pandas as pd
from cioos_data_transform.parse.seabird import (
    get_seabird_instrument_from_header,
    get_seabird_processing_history,
)
from cioos_data_transform.utils.xarray_methods import history_input

logger = logging.getLogger(__name__)

module_path = os.path.dirname(__file__)
profile_direction_map = {"DN": "downward", "FLAT": "stable", "UP": "upward"}
with open(
    os.path.join(module_path, "attribute_corrections.json"), encoding="UTF-8"
) as f:
    attribute_corrections = json.load(f)

# This could be potentially be replaced by using the NERC Server instead
reference_platforms = pd.read_csv(
    os.path.join(module_path, "reference_platforms.csv"),
    dtype={"wmo_platform_code": "string"},
)
platform_mapping = {key.lower(): key for key in reference_platforms["platform_name"]}

stationless_programs = ("Maritime Region Ecosystem Survey",)


def titleize(text):
    """Titlelize a string and ignore specific expressions"""
    do_not_change = ["AZMP", "(AZMP)", "ADCP", "(ADCP)", "CTD", "a", "the"]
    return " ".join(
        item.title() if item not in do_not_change else item for item in text.split(" ")
    )


def match_platform(platform):
    """Review ODF CRUISE_HEADER:PLATFORM and match to closest"""
    platform = re.sub(
        r"CCGS_*\s*|CGCB\s*|FRV\s*|NGCC\s*|^_|MV\s*", "", platform
    ).strip()
    matched_platform = get_close_matches(platform.lower(), platform_mapping.keys())
    if matched_platform:
        return (
            reference_platforms[
                reference_platforms["platform_name"]
                == platform_mapping[matched_platform[0]]
            ]
            .iloc[0]
            .dropna()
            .to_dict()
        )
    logger.warning("Unknown platform %s", platform)
    return {}


def global_attributes_from_header(dataset, odf_header):
    """
    Method use to define the standard global attributes from an ODF Header
    parsed by the read function.
    """

    def _review_latitude(value):
        return value if value != -99.9 else None

    def _review_longitude(value):
        return value if value != -999.9 else None

    odf_original_header = odf_header.copy()
    odf_original_header.pop("variable_attributes")
    dataset.attrs.update(
        {
            "cruise_name": odf_header["CRUISE_HEADER"]["CRUISE_NAME"],
            "cruise_number": str(odf_header["CRUISE_HEADER"]["CRUISE_NUMBER"]),
            "cruise_description": odf_header["CRUISE_HEADER"]["CRUISE_DESCRIPTION"],
            "chief_scientist": standardize_chief_scientist(
                odf_header["CRUISE_HEADER"]["CHIEF_SCIENTIST"]
            ),
            "mission_start_date": odf_header["CRUISE_HEADER"].get("START_DATE"),
            "mission_end_date": odf_header["CRUISE_HEADER"].get("END_DATE"),
            "event_number": odf_header["EVENT_HEADER"]["EVENT_NUMBER"],
            "event_start_time": odf_header["EVENT_HEADER"]["START_DATE_TIME"],
            "event_end_time": odf_header["EVENT_HEADER"]["END_DATE_TIME"],
            "initial_latitude": _review_latitude(
                odf_header["EVENT_HEADER"]["INITIAL_LATITUDE"]
            ),
            "initial_longitude": _review_longitude(
                odf_header["EVENT_HEADER"]["INITIAL_LONGITUDE"]
            ),
            "end_latitude": _review_latitude(
                odf_header["EVENT_HEADER"]["END_LATITUDE"]
            ),
            "end_longitude": _review_longitude(
                odf_header["EVENT_HEADER"]["END_LONGITUDE"]
            ),
            "sampling_interval": odf_header["EVENT_HEADER"]["SAMPLING_INTERVAL"],
            "sounding": odf_header["EVENT_HEADER"]["SOUNDING"],
            "depth_off_bottom": odf_header["EVENT_HEADER"]["DEPTH_OFF_BOTTOM"],
            "date_created": odf_header["EVENT_HEADER"]["ORIG_CREATION_DATE"],
            "date_modified": odf_header["EVENT_HEADER"]["CREATION_DATE"],
            "history": "",
            "comments": odf_header["EVENT_HEADER"].get("EVENT_COMMENTS", ""),
            "original_odf_header": "\n".join(odf_header["original_header"]),
            "original_odf_header_json": json.dumps(
                odf_original_header, ensure_ascii=False, indent=False, default=str
            ),
        }
    )

    # Map PLATFORM to NERC C17
    dataset.attrs.update(match_platform(odf_header["CRUISE_HEADER"]["PLATFORM"]))

    # Convert ODF history to CF history
    is_manufacturer_header = False
    dataset.attrs["instrument_manufacturer_header"] = ""
    dataset.attrs["internal_processing_notes"] = ""
    for history_group in odf_header["HISTORY_HEADER"]:
        # Convert single processes to list
        if isinstance(history_group["PROCESS"], str):
            history_group["PROCESS"] = [history_group["PROCESS"]]

        for row in history_group["PROCESS"]:
            # Retrieve Instrument Manufacturer Header
            if row.startswith("* Sea-Bird"):
                dataset.attrs["history"] += "# SEA-BIRD INSTRUMENTS HEADER\n"
                is_manufacturer_header = True
            if is_manufacturer_header:
                dataset.attrs["instrument_manufacturer_header"] += row + "\n"
            else:
                dataset.attrs["internal_processing_notes"] += history_input(
                    row, history_group["CREATION_DATE"]
                )

            # End of manufacturer header
            if row.startswith("*END*"):
                is_manufacturer_header = False
                dataset.attrs["history"] += "# ODF Internal Processing Notes\n"

            # Ignore some specific lines within the history (mostly seabird header ones)
            if re.match(
                r"^(\#\s*\<.*|\*\* .*"
                + r"|\# (name|span|nquan|nvalues|unit|interval|start_time|bad_flag)"
                + r"|\* |\*END\*)",
                row,
            ):
                continue
            # Add to cf history
            dataset.attrs["history"] += history_input(
                row, history_group["CREATION_DATE"]
            )

    # Instrument Specific Information
    if dataset.attrs["instrument_manufacturer_header"]:
        dataset.attrs["instrument"] = get_seabird_instrument_from_header(
            dataset.attrs["instrument_manufacturer_header"]
        )
        dataset.attrs["seabird_processing_modules"] = get_seabird_processing_history(
            dataset.attrs["instrument_manufacturer_header"]
        )
    elif "INSTRUMENT_HEADER" in odf_header:
        dataset.attrs[
            "instrument"
        ] = f"{odf_header['INSTRUMENT_HEADER']['INST_TYPE']} {odf_header['INSTRUMENT_HEADER']['MODEL']}"
        dataset.attrs["instrument_serial_number"] = odf_header["INSTRUMENT_HEADER"][
            "SERIAL_NUMBER"
        ]
    else:
        logger.warning("No Instrument field available")
        dataset.attrs["instrument"] = ""
        dataset.attrs["instrument_serial_number"] = ""

    if re.search(
        r"(SBE\s*(9|16|19|25|37))|CTD|Guildline|GUILDLN",
        dataset.attrs["instrument"],
        re.IGNORECASE,
    ):
        dataset.attrs["instrument_type"] = "CTD"
    elif re.search(r"Bathythermograph Manual", dataset.attrs["instrument"]):
        dataset.attrs["instrument_type"] = "BT"
    else:
        logger.warning(
            "Unknown instrument type for instrument: %s; odf['INSTRUMENT_HEADER']: %s",
            dataset.attrs["instrument"],
            odf_header.get("INSTRUMENT_HEADER"),
        )

    # Derive cdm_data_type from DATA_TYPE
    type_profile = {"CTD": "CTD", "BOTL": "Bottle", "BT": "Bottle"}
    data_type = odf_header["EVENT_HEADER"]["DATA_TYPE"]
    if data_type in ["CTD", "BOTL", "BT"]:
        dataset.attrs["cdm_data_type"] = "Profile"
        dataset.attrs["cdm_profile_variables"] = ""
        if data_type == "CTD":
            dataset.attrs["profile_direction"] = odf_header["EVENT_HEADER"][
                "EVENT_QUALIFIER2"
            ]
        dataset.attrs["title"] = (
            f"{type_profile[data_type]} profile data collected "
            + f"from the {dataset.attrs['platform']} {dataset.attrs['platform_name']} by "
            + f"{dataset.attrs['organization']}  {dataset.attrs['institution']} "
            + f"on the {dataset.attrs['cruise_name'].title()} "
        )
        if (
            dataset.attrs["mission_start_date"]
            and dataset.attrs["mission_end_date"]
            and isinstance(dataset.attrs["mission_start_date"], datetime)
            and isinstance(dataset.attrs["mission_end_date"], datetime)
        ):
            dataset.attrs["title"] += (
                f"from {dataset.attrs['mission_start_date'].strftime('%d-%b-%Y')} "
                + f"to {dataset.attrs['mission_end_date'].strftime('%d-%b-%Y')}."
            )

    else:
        logger.error(
            "ODF_transform is not yet incompatible with ODF DATA_TYPE: %s",
            odf_header["EVENT_HEADER"]["DATA_TYPE"],
        )

    ## FIX ODF ATTRIBUTES
    # # event_number should be number otherwise get rid of it
    if isinstance(dataset.attrs["event_number"], int):
        event_number = re.search(
            r"\*\* Event[\s\:\#]*(\d+)",
            "".join(odf_header["original_header"]),
            re.IGNORECASE,
        )
        if event_number:
            dataset.attrs["event_number"] = int(event_number[1])
        else:
            logger.warning(
                "event_number %s is not just a number",
                dataset.attrs["event_number"],
            )
            dataset.attrs.pop("event_number")

    # Search station anywhere within ODF Header
    station = re.search(
        r"station[\w\s]*:\s*(\w*)",
        "".join(odf_header["original_header"]),
        re.IGNORECASE,
    )
    if (
        station
        and not "station" in dataset.attrs
        and dataset.attrs.get("project", "") not in stationless_programs
    ):
        station = station[1].strip()

        # Standardize stations with convention AA02, AA2 and AA_02 to AA02
        if re.match(r"[A-Za-z]+\_*\d+", station):
            station_items = re.search(r"([A-Za-z]+)_*(\d+)", station).groups()
            dataset.attrs[
                "station"
            ] = f"{station_items[0].upper()}{int(station_items[1]):02g}"
        # Station is just number convert to string with 001
        elif re.match(r"^[0-9]+$", station):
            # Ignore station that are actually the event_number
            if int(station) != dataset.attrs.get("event_number"):
                logger.warning(
                    "Station name is suspicious since its just a number: %s",
                    station,
                )
                dataset.attrs["station"] = f"{int(station):03g}"
        else:
            dataset.attrs["station"] = station

    # Standardize project and cruise_name (AZMP, AZOMP and MARES)
    if dataset.attrs.get("program") == "Atlantic Zone Monitoring Program":
        if dataset.attrs.get("project") is None:
            season = (
                "Spring"
                if 1 <= dataset.attrs["event_start_time"].month <= 7
                else "Fall"
            )
            dataset.attrs["project"] = f"{dataset.attrs.get('program')} {season}"
            dataset.attrs[
                "cruise_name"
            ] = f"{dataset.attrs['project']} {dataset.attrs['event_start_time'].year}"
        elif "cruise_name" in dataset.attrs:
            # Ignore cruise_name for station specific AZMP projects
            dataset.attrs.pop("cruise_name")
    elif dataset.attrs.get("program") == "Maritime Region Ecosystem Survey":
        season = (
            "Summer" if 5 <= dataset.attrs["event_start_time"].month <= 9 else "Winter"
        )
        dataset.attrs["project"] = f"{dataset.attrs.get('program')} {season}"
        dataset.attrs[
            "cruise_name"
        ] = f"{dataset.attrs['project']} {dataset.attrs['event_start_time'].year}"
    elif dataset.attrs.get("program") == "Atlantic Zone Off-Shelf Monitoring Program":
        dataset.attrs[
            "cruise_name"
        ] = f"{dataset.attrs['program']} {dataset.attrs['event_start_time'].year}"

    # Apply attributes corrections from attribute_correction json
    for att, items in attribute_corrections.items():
        if att in dataset.attrs:
            for key, value in items.items():
                dataset.attrs[att] = dataset.attrs[att].replace(key, value)

    # Review attributes format
    for attr in ["event_start_time", "event_end_time"]:
        if dataset.attrs.get(attr) not in (None, pd.NaT) and not isinstance(
            dataset.attrs[attr], datetime
        ):
            logging.warning(
                "%s failed to be converted to timestamp: %s",
                attr,
                dataset.attrs[attr],
            )
        elif dataset.attrs[attr] < pd.Timestamp(1900, 1, 1).tz_localize("UTC"):
            logging.warning("%s is before 1900-01-01 which is very suspicious", attr)

    # Drop empty attributes
    dataset.attrs = {
        key: value
        for key, value in dataset.attrs.items()
        if value not in (None, pd.NaT)
    }
    return dataset


def retrieve_coordinates(dataset):
    """
    Method use to generate metadata variables from the ODF Header to a xarray Dataset.
    """

    if dataset.attrs["cdm_data_type"] == "Profile":
        dataset["time"] = dataset.attrs["event_start_time"]
        dataset["latitude"] = dataset.attrs["initial_latitude"]
        dataset["longitude"] = dataset.attrs["initial_longitude"]
        # depth is generated by vocabulary
    else:
        logger.error(
            "odf_converter is not yet compatible with %s",
            dataset.attrs["cdm_data_type"],
        )
    # Apply attributes to each coordinate variables
    coordinate_attributes = {
        "time": {
            "name": "time",
            "standard_name": "time",
            "ioos_category": "Time",
            "coverage_content_type": "coordinate",
        },
        "latitude": {
            "long_name": "Latitude",
            "units": "degrees_north",
            "standard_name": "latitude",
            "ioos_category": "Location",
            "coverage_content_type": "coordinate",
        },
        "longitude": {
            "long_name": "Longitude",
            "units": "degrees_east",
            "standard_name": "longitude",
            "ioos_category": "Location",
            "coverage_content_type": "coordinate",
        },
    }
    for var, attrs in coordinate_attributes.items():
        if var in dataset:
            dataset[var].attrs = attrs
    return dataset


def standardize_chief_scientist(name):
    """Apply minor corrections to chief_scientist"""
    name = re.sub(r"\s+(\~|\/)", ",", name)
    return re.sub(r"(^|\s)(d|D)r\.{0,1}", "", name).strip().title()
