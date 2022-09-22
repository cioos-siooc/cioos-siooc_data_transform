"""
Attribute module regroup all the different tools used to standardize the ODFs
attribtutes to the different conventions (CF, ACDD).
"""

import json
import logging
import re
from datetime import datetime, timezone
from difflib import get_close_matches

import pandas as pd
from odf_transform.utils.seabird import (
    get_seabird_instrument_from_header,
    get_seabird_processing_history,
)

no_file_logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(no_file_logger, {"file": None})

stationless_programs = ("Maritime Region Ecosystem Survey",)


def _generate_platform_attributes(platform, reference_platforms):
    """Review ODF CRUISE_HEADER:PLATFORM and match to closest"""
    platform = re.sub(
        r"CCGS_*\s*|CGCB\s*|FRV\s*|NGCC\s*|^_|MV\s*", "", platform
    ).strip()
    matched_platform = get_close_matches(platform.lower(), reference_platforms.keys())
    if matched_platform:
        return reference_platforms[matched_platform[0]]
    logger.warning("Unknown platform %s", platform)
    return {}


def _generate_cf_history_from_odf(odf_header):
    """
    Generate from ODF HISTORY_HEADER, CF recommended format history attribute.
    If a Seabird instrument csv header is provided, it will be converted to a CF standard and
    made available within the instrument_manufacturer_header attribute.
    Processing steps associated with the SBE Processing toolbox will also be
    incorporated within the history attribute.
    """

    def _add_to_history(comment, date=datetime.now(timezone.utc)):
        """Generate a CF standard history line."""
        return f"{date.strftime('%Y-%m-%dT%H:%M:%SZ')} {comment}\n"

    # Convert ODF history to CF history
    is_manufacturer_header = False

    history = {
        "instrument_manufacturer_header": "",
        "internal_processing_notes": "",
        "history": "",
    }
    for history_group in odf_header["HISTORY_HEADER"]:
        # Convert single processes to list
        if isinstance(history_group["PROCESS"], str):
            history_group["PROCESS"] = [history_group["PROCESS"]]

        # Empty history group (just write the date)
        if history_group["PROCESS"] is None:
            history["history"] += _add_to_history("", history_group["CREATION_DATE"])
            continue

        for row in history_group["PROCESS"]:
            if row is None:
                continue
            # Retrieve Instrument Manufacturer Header
            if row.startswith("* Sea-Bird"):
                history["history"] += "# SEA-BIRD INSTRUMENTS HEADER\n"
                is_manufacturer_header = True
            if is_manufacturer_header:
                history["instrument_manufacturer_header"] += row + "\n"
            else:
                history["internal_processing_notes"] += _add_to_history(
                    row, history_group["CREATION_DATE"]
                )

            # End of manufacturer header
            if row.startswith("*END*"):
                is_manufacturer_header = False
                history["history"] += "# ODF Internal Processing Notes\n"

            # Ignore some specific lines within the history (mostly seabird header ones)
            if re.match(
                r"^(\#\s*\<.*|\*\* .*"
                + r"|\# (name|span|nquan|nvalues|unit|interval|start_time|bad_flag)"
                + r"|\* |\*END\*)",
                row,
            ):
                continue
            # Add to cf history
            history["history"] += _add_to_history(row, history_group["CREATION_DATE"])
    return history


def _define_cdm_data_type_from_odf(odf_header):
    """Generate cdm_data_type attributes based on the odf data_type attribute."""
    # Derive cdm_data_type from DATA_TYPE
    odf_data_type = odf_header["EVENT_HEADER"]["DATA_TYPE"]
    attributes = {"odf_data_type": odf_data_type}
    if odf_data_type in ["CTD", "BOTL", "BT"]:
        attributes.update(
            {
                "cdm_data_type": "Profile",
                "cdm_profile_variables": "",
            }
        )

        if odf_data_type == "CTD":
            attributes["profile_direction"] = odf_header["EVENT_HEADER"][
                "EVENT_QUALIFIER2"
            ]

    else:
        logger.error(
            "ODF_transform is not yet incompatible with ODF DATA_TYPE: %s",
            odf_data_type,
        )
    return attributes


def _review_event_number(global_attributes, odf_header):
    """Review event_number which should be number otherwise get rid of it"""
    # If interger already return that same value
    if isinstance(global_attributes["event_number"], int):
        return global_attributes["event_number"]
    elif isinstance(global_attributes["event_number"], str) and re.match(
        r"\d+P", global_attributes["event_number"]
    ):
        return int(global_attributes["event_number"].replace("P", ""))

    # Look for an event_number withih all the original header
    event_number = re.search(
        r"\*\* Event[\s\:\#]*(\d+)",
        "".join(odf_header["original_header"]),
        re.IGNORECASE,
    )
    if event_number:
        return int(event_number[1])
    logger.warning(
        "event_number %s is not just a number",
        global_attributes["event_number"],
    )


def _standardize_station_names(station):
    """
    Standardize stations with convention:
        - ABC01: capital letters two digits
        - 001: 3 digits numbers
        - Otherwise unchanged
    """
    if re.match(r"[A-Za-z]+\_*\d+", station):
        station_items = re.search(r"([A-Za-z]+)_*(\d+)", station).groups()
        return f"{station_items[0].upper()}{int(station_items[1]):02g}"
    # Station is just number convert to string with 001
    elif re.match(r"^[0-9]+$", station):
        return f"{int(station):03g}"
    else:
        return station


def _review_station(global_attributes, odf_header):
    """Review station attribute,
    - If not available search in original odf header for "station... : STATION_NAME"
    - Standardize station name
    - Make sure station != event_number
    """
    # If station is already available return it back
    if "station" in global_attributes:
        return global_attributes["station"]
    elif global_attributes.get("project", "") not in stationless_programs:
        return None

    # Search station anywhere within ODF Header
    station = re.search(
        r"station[\w\s]*:\s*(\w*)",
        "".join(odf_header["original_header"]),
        re.IGNORECASE,
    )
    if station is None:
        return

    # If station is found standardize it
    station = station[1].strip()

    # Ignore station that are actually the event_number
    if re.match(r"^[0-9]+$", station) and int(station) != global_attributes.get(
        "event_number"
    ):
        logger.warning(
            "Station name is suspicious since its just a number: %s",
            station,
        )
        return

    return _standardize_station_names(station)


def _review_time_attributes(value, attr):
    """Review time attributesw which should be:
    - Parsed and converted to datetime
    - > 1900-01-01
    """
    # Review attributes format
    if value in (None, pd.NaT, ""):
        return pd.NaT
    if not isinstance(value, datetime):
        logger.warning(
            "Failed to convert timestamp %s: %s",
            attr,
            value,
        )
        return pd.NaT
    elif value < pd.Timestamp(1900, 1, 1).tz_localize("UTC"):
        logger.warning(
            "%s = %s is before 1900-01-01 which is very suspicious", attr, value
        )
    return value


def _generate_instrument_attributes(odf_header, instrument_manufacturer_header=None):
    """
    Generate instrument attributes based on:
    - ODF instrument attribute
    - manufacturer header
    """
    # Instrument Specific Information
    if "INSTRUMENT_HEADER" not in odf_header or (
        odf_header["INSTRUMENT_HEADER"].get("INST_TYPE")
        and odf_header["INSTRUMENT_HEADER"].get("MODEL")
        and odf_header["INSTRUMENT_HEADER"].get("SERIAL_NUMBER")
    ):
        logger.info("No instrument information available")
        return {}
    attributes = {}
    if instrument_manufacturer_header:
        attributes["instrument"] = get_seabird_instrument_from_header(
            instrument_manufacturer_header
        )
        attributes["seabird_processing_modules"] = get_seabird_processing_history(
            instrument_manufacturer_header
        )
    elif "INSTRUMENT_HEADER" in odf_header:
        attributes["instrument"] = " ".join(
            [
                odf_header["INSTRUMENT_HEADER"].get("INST_TYPE") or "",
                odf_header["INSTRUMENT_HEADER"].get("MODEL") or "",
            ]
        ).strip()
        attributes["instrument_serial_number"] = (
            odf_header["INSTRUMENT_HEADER"].get("SERIAL_NUMBER") or ""
        )

    # Attempt to generate an instrument_type attribute
    if re.search(
        r"(SBE\s*(9|16|19|25|37))|CTD|Guildline|GUILDLN|STD",
        attributes["instrument"],
        re.IGNORECASE,
    ):
        attributes["instrument_type"] = "CTD"
    elif re.search(r"Bathythermograph Manual", attributes["instrument"]):
        attributes["instrument_type"] = "BT"
    else:
        logger.warning(
            "Unknown instrument type for instrument: %s; odf['INSTRUMENT_HEADER']: %s",
            attributes["instrument"],
            odf_header.get("INSTRUMENT_HEADER"),
        )
    return attributes


def _generate_title_from_global_attributes(attributes):
    title = (
        f"{attributes['odf_data_type']} profile data collected "
        + (
            f"from the {attributes['platform']} {attributes['platform_name']}"
            if "platform" in attributes and "platform_name" in attributes
            else ""
        )
        + f"by {attributes['organization']}  {attributes['institution']} "
        + f"on the {attributes['cruise_name'].title()} "
    )
    if (
        pd.notna(attributes["mission_start_date"])
        and pd.notna(attributes["mission_end_date"])
        and isinstance(attributes["mission_start_date"], datetime)
        and isinstance(attributes["mission_end_date"], datetime)
    ):
        title += (
            f"from {attributes['mission_start_date'].strftime('%d-%b-%Y')} "
            + f"to {attributes['mission_end_date'].strftime('%d-%b-%Y')}."
        )
    return title


def _generate_program_specific_attritutes(global_attributes):
    """Generate program specific attributes
    Bedford Institute of Oceanography
    - AZMP
        + Program specific -> cruise_name = None
    - MARES
    - AZOMP
    """
    # Standardize project and cruise_name (AZMP, AZOMP and MARES)
    if "program" not in global_attributes:
        return {}

    program = global_attributes["program"]
    project = global_attributes.get("project")
    year = global_attributes["event_start_time"].year
    month = global_attributes["event_start_time"].month

    if program == "Atlantic Zone Monitoring Program":
        season = "Spring" if 1 <= month <= 7 else "Fall"
        return {
            "project": project or f"{program} {season}",
            "cruise_name": None if project else f"{program} {season} {year}",
        }

    elif program == "Maritime Region Ecosystem Survey":
        season = "Summer" if 5 <= month <= 9 else "Winter"
        return {
            "project": f"{program} {season}",
            "cruise_name": f"{program} {season} {year}",
        }
    elif program in [
        "Atlantic Zone Off-Shelf Monitoring Program",
        "Barrow Strait Monitoring Program",
    ]:
        return {"cruise_name": f"{program} {year}"}
    else:
        return {}


def global_attributes_from_header(dataset, odf_header, config=None):
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
    platform_attributes = _generate_platform_attributes(
        odf_header["CRUISE_HEADER"]["PLATFORM"], config["reference_platforms"]
    )
    history = _generate_cf_history_from_odf(odf_header)
    instrument_attributes = _generate_instrument_attributes(
        odf_header, history.get("instrument_manufacturer_header")
    )
    cdm_data_type_attributes = _define_cdm_data_type_from_odf(odf_header)
    dataset.attrs.update(
        {
            "cruise_name": odf_header["CRUISE_HEADER"]["CRUISE_NAME"],
            "cruise_number": str(odf_header["CRUISE_HEADER"]["CRUISE_NUMBER"]),
            "cruise_description": odf_header["CRUISE_HEADER"]["CRUISE_DESCRIPTION"],
            "chief_scientist": _standardize_chief_scientist(
                odf_header["CRUISE_HEADER"]["CHIEF_SCIENTIST"]
            ),
            "mission_start_date": _review_time_attributes(
                odf_header["CRUISE_HEADER"].get("START_DATE"), "START_DATE"
            ),
            "mission_end_date": _review_time_attributes(
                odf_header["CRUISE_HEADER"].get("END_DATE"), "END_DATE"
            ),
            "event_number": odf_header["EVENT_HEADER"]["EVENT_NUMBER"],
            "event_start_time": _review_time_attributes(
                odf_header["EVENT_HEADER"]["START_DATE_TIME"], "START_DATE_TIME"
            ),
            "event_end_time": _review_time_attributes(
                odf_header["EVENT_HEADER"]["END_DATE_TIME"], "END_DATE_TIME"
            ),
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
            "date_created": pd.Timestamp.utcnow(),
            "date_modified": odf_header["EVENT_HEADER"]["CREATION_DATE"],
            "date_issued": odf_header["EVENT_HEADER"]["ORIG_CREATION_DATE"],
            "history": "",
            "comments": odf_header["EVENT_HEADER"].get("EVENT_COMMENTS"),
            "original_odf_header": "\n".join(odf_header["original_header"]),
            "original_odf_header_json": json.dumps(
                odf_original_header, ensure_ascii=False, indent=False, default=str
            ),
            **platform_attributes,
            **instrument_attributes,
            **history,
            **cdm_data_type_attributes,
        }
    )
    # Generate attributes from other attributes
    dataset.attrs["title"] = _generate_title_from_global_attributes(dataset.attrs)
    dataset.attrs.update(**_generate_program_specific_attritutes(dataset.attrs))

    # Review ATTRIBUTES
    dataset.attrs["event_number"] = _review_event_number(dataset.attrs, odf_header)
    dataset.attrs["station"] = _review_station(dataset.attrs, odf_header)
    if isinstance(dataset.attrs["comments"], list):
        dataset.attrs["comments"] = "\n".join(
            [line for line in dataset.attrs["comments"] if line]
        )

    # Apply attributes corrections from attribute_correction json
    dataset.attrs.update(
        {
            attr: attr_mapping[dataset.attrs[attr]]
            for attr, attr_mapping in config["attribute_mapping_corrections"].items()
            if attr in dataset.attrs and dataset.attrs[attr] in attr_mapping
        }
    )

    # Drop empty global attributes
    dataset.attrs = {
        key: value
        for key, value in dataset.attrs.items()
        if value not in (None, pd.NaT)
    }
    return dataset


def generate_coordinates_variables(dataset):
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


def _standardize_chief_scientist(name):
    """Apply minor corrections to chief_scientist
    - replace separator ~, / by ,
    - Ignore Dr.
    """
    name = re.sub(r"\s+(\~|\/)", ",", name)
    return re.sub(r"(^|\s)(d|D)r\.{0,1}", "", name).strip().title()
