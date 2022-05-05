from datetime import datetime
import pandas as pd
import re
import json
import os
from .parser import convert_odf_time
from cioos_data_transform.utils.xarray_methods import history_input
from cioos_data_transform.parse.seabird import (
    get_seabird_instrument_from_header,
    get_seabird_processing_history,
)
from difflib import get_close_matches
import logging

logger = logging.getLogger(__name__)

module_path = os.path.dirname(__file__)
profile_direction_map = {"DN": "downward", "FLAT": "stable", "UP": "upward"}
with open(os.path.join(module_path, "attribute_corrections.json")) as f:
    attribute_corrections = json.load(f)

# This could be potentially be replaced by using the NERC Server instead
reference_vessel = pd.read_csv(
    os.path.join(module_path, "reference_vessel.csv"),
    dtype={"wmo_platform_code": "string"},
)
platform_mapping = {key.lower(): key for key in reference_vessel["platform_name"]}

institute_attributes = [
    "institution",
    "institution_fr",
    "country",
    "ioc_country_code",
    "iso_3166_country_code",
    "ices_edmo_code",
    "sdn_institution_urn",
]
platform_attributes = ["platform_name", "sdn_platform_urn", "wmo_platform_code"]


def titleize(text):
    do_not_change = ["AZMP", "(AZMP)", "ADCP", "(ADCP)", "CTD", "a", "the"]
    return " ".join(
        item.title() if item not in do_not_change else item for item in text.split(" ")
    )


def match_platform(platform):
    """Review ODF CRUISE_HEADER:PLATFORM and match to closest"""
    platform = re.sub("CCGS_*\s*|CGCB\s*|FRV\s*|NGCC\s*|^_|MV\s*", "", platform).strip()
    matched_vessel = get_close_matches(platform.lower(), platform_mapping.keys())
    if matched_vessel:
        return (
            reference_vessel[
                reference_vessel["platform_name"] == platform_mapping[matched_vessel[0]]
            ]
            .iloc[0]
            .dropna()
            .to_dict()
        )
    else:
        logger.warning(f"Unknown platform {platform}")
        return {}


def global_attributes_from_header(ds, odf_header):
    """
    Method use to define the standard global attributes from an ODF Header parsed by the read function.
    """

    def _reviewLat(value):
        return value if value != -99.9 else None

    def _reviewLon(value):
        return value if value != -999.9 else None

    odf_original_header = odf_header.copy()
    odf_original_header.pop("variable_attributes")
    ds.attrs.update(
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
            "initial_latitude": _reviewLat(
                odf_header["EVENT_HEADER"]["INITIAL_LATITUDE"]
            ),
            "initial_longitude": _reviewLon(
                odf_header["EVENT_HEADER"]["INITIAL_LONGITUDE"]
            ),
            "end_latitude": _reviewLat(odf_header["EVENT_HEADER"]["END_LATITUDE"]),
            "end_longitude": _reviewLon(odf_header["EVENT_HEADER"]["END_LONGITUDE"]),
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
    ds.attrs.update(match_platform(odf_header["CRUISE_HEADER"]["PLATFORM"]))

    # Convert ODF history to CF history
    is_manufacturer_header = False
    ds.attrs["instrument_manufacturer_header"] = ""
    ds.attrs["internal_processing_notes"] = ""
    ds.attrs["seabird_processing_modules"] = ""
    for history_group in odf_header["HISTORY_HEADER"]:
        # Convert single processes to list
        if type(history_group["PROCESS"]) is str:
            history_group["PROCESS"] = [history_group["PROCESS"]]

        for row in history_group["PROCESS"]:
            # Retrieve Instrument Manufacturer Header
            if row.startswith("* Sea-Bird"):
                ds.attrs["history"] += "# SEA-BIRD INSTRUMENTS HEADER\n"
                is_manufacturer_header = True
            if is_manufacturer_header:
                ds.attrs["instrument_manufacturer_header"] += row + "\n"
            else:
                ds.attrs["internal_processing_notes"] += history_input(
                    row, history_group["CREATION_DATE"]
                )

            # End of manufacturer header
            if row.startswith("*END*"):
                is_manufacturer_header = False
                ds.attrs["history"] += "# ODF Internal Processing Notes\n"

            # Ignore some specific lines within the history (mostly seabird header ones)
            if re.match(
                "^(\#\s*\<.*|\*\* .*|\# (name|span|nquan|nvalues|unit|interval|start_time|bad_flag)|\* |\*END\*)",
                row,
            ):
                continue
            # Add to history
            ds.attrs["history"] += history_input(row, history_group["CREATION_DATE"])

    # Instrument Specific Information
    if ds.attrs["instrument_manufacturer_header"]:
        ds.attrs["instrument"] = get_seabird_instrument_from_header(
            ds.attrs["instrument_manufacturer_header"]
        )
        ds.attrs["seabid_processing_modules"] = get_seabird_processing_history(
            ds.attrs["instrument_manufacturer_header"]
        )
    elif "INSTRUMENT_HEADER" in odf_header:
        ds.attrs[
            "instrument"
        ] = f'{odf_header["INSTRUMENT_HEADER"]["INST_TYPE"]} {odf_header["INSTRUMENT_HEADER"]["MODEL"]}'
        ds.attrs["instrument_serial_number"] = odf_header["INSTRUMENT_HEADER"][
            "SERIAL_NUMBER"
        ]
    else:
        logger.warning(f"No Instrument field available")
        ds.attrs["instrument"] = ""
        ds.attrs["instrument_serial_number"] = ""

    if re.search(
        "(SBE\s*(9|16|19|25|37))|CTD|Guildline|GUILDLN",
        ds.attrs["instrument"],
        re.IGNORECASE,
    ):
        ds.attrs["instrument_type"] = "CTD"
    elif re.search("Bathythermograph Manual", ds.attrs["instrument"]):
        ds.attrs["instrument_type"] = "BT"
    else:
        logger.warning(
            f"Unknown instrument type for instrument: {ds.attrs['instrument']}; odf['INSTRUMENT_HEADER']: {odf_header.get('INSTRUMENT_HEADER')}"
        )

    # TODO map instrument to seadatanet L22 instrument

    # Derive cdm_data_type from DATA_TYPE
    type_profile = {"CTD": "CTD", "BOTL": "Bottle", "BT": "Bottle"}
    data_type = odf_header["EVENT_HEADER"]["DATA_TYPE"]
    if data_type in ["CTD", "BOTL", "BT"]:
        ds.attrs["cdm_data_type"] = "Profile"
        ds.attrs["cdm_profile_variables"] = ""
        if data_type == "CTD":
            ds.attrs["profile_direction"] = odf_header["EVENT_HEADER"][
                "EVENT_QUALIFIER2"
            ]
        ds.attrs["title"] = (
            f"{type_profile[data_type]} profile data collected "
            + f"from the {ds.attrs['platform']} {ds.attrs['platform_name']} by "
            + f"{ds.attrs['organization']}  {ds.attrs['institution']} "
            + f"on the {ds.attrs['cruise_name'].title()} "
        )
        if (
            ds.attrs["mission_start_date"]
            and ds.attrs["mission_end_date"]
            and type(ds.attrs["mission_start_date"]) is datetime
            and type(ds.attrs["mission_end_date"]) is datetime
        ):
            ds.attrs["title"] += (
                f"from {ds.attrs['mission_start_date'].strftime('%d-%b-%Y')} "
                + f"to {ds.attrs['mission_end_date'].strftime('%d-%b-%Y')}."
            )

    else:
        logger.error(
            f"ODF_transform is not yet incompatible with ODF DATA_TYPE: {odf_header['EVENT_HEADER']['DATA_TYPE']}"
        )

    ## FIX ODF ATTRIBUTES
    # Chief scientists
    ds.attrs["chief_scientist"] = re.sub("\s+(\~|\/)", ",", ds.attrs["chief_scientist"])

    # event_number should be number otherwise get rid of it
    if type(ds.attrs["event_number"]) is not int:
        ds.attrs.pop("event_number")

    # Search anywhere within ODF Header
    station = re.search(
        "station[\w\s]*:\s*(\w*)", "".join(odf_header["original_header"]), re.IGNORECASE
    )
    if station and not "station" in ds.attrs:
        station = station[1].strip()

        # Standardize stations with convention AA02, AA2 and AA_02 to AA02
        if re.match("[A-Za-z]+\_*\d+", station):
            station_items = re.search("([A-Za-z]+)_*(\d+)", station).groups()
            ds.attrs[
                "station"
            ] = f"{station_items[0].upper()}{int(station_items[1]):02g}"
        # Station is just number convert to string with 001
        elif re.match("^[0-9]+$", station):
            # Ignore station that are actually the event_number
            if int(station)!=ds.attrs.get('event_number'):
                ds.attrs["station"] = f"{int(station):03g}"
        else:
            ds.attrs["station"] = station

    # Overwrite cruise_name to format "{program} {season [optional, AZMP]} {year}" format if program exist
    if "program" in ds.attrs:
        cruise_name = [ds.attrs["program"]]
        if ds.attrs["program"] == "Atlantic Zone Monitoring Program":
            cruise_name += [
                "Spring" if 1 <= ds.attrs["event_start_time"].month <= 7 else "Fall"
            ]
        elif ds.attrs["program"] == "Groundfish":
            cruise_name += [
                "Summer" if 5 <= ds.attrs["event_start_time"].month <= 9 else "Winter"
            ]
        # Add program {season} to as project for some specific programs
        if ds.attrs["program"] in ("Atlantic Zone Monitoring Program","Groundfish"):
            ds.attrs["project"] = cruise_name

        cruise_name += [str(ds.attrs["event_start_time"].year)]
        ds.attrs["cruise_name"] = " ".join(cruise_name)

    # Apply attributes corrections from attribute_correction json
    for att, items in attribute_corrections.items():
        if att in ds.attrs:
            for key, value in items.items():
                ds.attrs[att] = ds.attrs[att].replace(key, value)

    # Review attributes format
    for attr in ['event_start_time','event_end_time']:
        if ds.attrs.get(attr) not in  (None, pd.NaT) and  type(ds.attrs[attr]) is not pd.Timestamp:
            logging.warning(f"{attr} failed to be converted to timestamp: {ds.attrs[attr]}")

    # Drop empty attributes
    ds.attrs = {key:value for key,value in ds.attrs.items() if value not in (None,pd.NaT)}
    return ds


def generate_variables_from_header(ds, odf_header):
    """
    Method use to generate metadata variables from the ODF Header to a xarray Dataset.
    """
    initial_variable_order = list(ds.keys())

    # General Attributes
    attrs_to_var = {
        "institution": {"ioos_category": "Other"},
        "cruise_name": {"ioos_category": "Other"},
        "cruise_number": {"ioos_category": "Other"},
        "chief_scientist": {"ioos_category": "Other"},
        "wmo_platform_code": {
            "name": "platform_id",
            "ioos_category": "Other",
            "standard_name": "platform_id",
            "dtype": str,
        },
        "platform": {
            "name": "platform",
            "ioos_category": "Other",
            "standard_name": "platform",
        },
        "platform": {
            "name": "platform_name",
            "ioos_category": "Other",
            "standard_name": "platform_name",
        },
        "event_number": {"dtype": str, "ioos_category": "Other"},
        "id": {"ioos_category": "Identifier"},
        "station": {"ioos_category": "Location"},
        "event_start_time": {"ioos_category": "Time"},
        "event_end_time": {"ioos_category": "Time"},
        "initial_latitude": {"units": "degrees_east", "ioos_category": "Location"},
        "initial_longitude": {"units": "degrees_east", "ioos_category": "Location"},
    }

    if ds.attrs["cdm_data_type"] == "Profile":
        # Define profile specific variables
        attrs_to_var.update(
            {
                "id": {"cf_role": "profile_id", "ioos_category": "Other"},
                "profile_direction": {"ioos_category": "Other"},
                "event_start_time": [
                    {
                        "name": "time",
                        "standard_name": "time",
                        "ioos_category": "Time",
                        "coverage_content_type": "coordinate",
                    },
                    {
                        "name": "profile_start_time",
                        "long_name": "Profile Start Time",
                        "ioos_category": "Time",
                        "coverage_content_type": "auxiliaryInformation",
                    },
                ],
                "event_end_time": {
                    "name": "profile_end_time",
                    "long_name": "Profile End Time",
                    "ioos_category": "Time",
                    "coverage_content_type": "auxiliaryInformation",
                },
                "initial_latitude": {
                    "name": "latitude",
                    "long_name": "Latitude",
                    "units": "degrees_north",
                    "standard_name": "latitude",
                    "ioos_category": "Location",
                    "coverage_content_type": "coordinate",
                },
                "initial_longitude": {
                    "name": "longitude",
                    "long_name": "Longitude",
                    "units": "degrees_east",
                    "standard_name": "longitude",
                    "ioos_category": "Location",
                    "coverage_content_type": "coordinate",
                },
            }
        )
        ds.attrs["cdm_profile_variables"] = ",".join(attrs_to_var.keys())

    # Add new variables and attributes
    for key, attrs in attrs_to_var.items():
        if type(attrs) is dict:
            attrs = [attrs]
        for att in attrs:
            new_key = att.pop("name") if "name" in att else key
            dtype = att.pop("dtype") if "dtype" in att else None
            # Ignore empty keys
            if key not in ds.attrs or ds.attrs[key] in (None, pd.NaT):
                continue

            ds[new_key] = ds.attrs[key]
            ds[new_key].attrs = att
            if dtype:
                ds[new_key] = ds[new_key].astype(dtype)

    # Reorder variables
    variable_list = [var for var in ds.keys() if var not in initial_variable_order]
    variable_list.extend(initial_variable_order)
    ds = ds[variable_list]
    return ds


def standardize_chief_scientist(name):
    return re.sub("(^|\s)(d|D)r\.{0,1}", "", name).strip().title()
