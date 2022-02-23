import pandas as pd
import re
import json
import os
from .parser import convert_odf_time
from cioos_data_transform.utils.xarray_methods import history_input
from cioos_data_transform.parse.seabird import get_seabird_instrument_from_header
import logging

logger = logging.getLogger(__name__)

module_path = os.path.dirname(__file__)
profile_direction_map = {"DN": "downward", "FLAT": "stable", "UP": "upward"}
reference_institutes = pd.read_csv(
    os.path.join(module_path, "reference_institute.csv"),
    dtype={"ices_edmo_code": "Int64", "ioc_country_code": "Int64"},
)
reference_vessel = pd.read_csv(
    os.path.join(module_path, "reference_vessel.csv"),
    dtype={"wmo_platform_code": "Int64"},
)

institute_attributes = [
    "institution",
    "institution_fr",
    "country",
    "ioc_country_code",
    "iso_3166_country_code",
    "ices_edmo_code",
    "sdn_institution_urn",
]
platform_attributes = ["platform", "sdn_platform_urn", "wmo_platform_code"]


def titleize(text):
    do_not_change = ["AZMP", "(AZMP)", "ADCP", "(ADCP)", "CTD", "a", "the"]
    return " ".join(
        item.title() if item not in do_not_change else item for item in text.split(" ")
    )


def match_institute(ices_code, institution):
    """Review ODF COUNTRY_INSTITUTE_CODE and ORGANIZATION and map to reference organization."""
    institution = re.sub("DFO\s*", "", institution)

    def _get_institute(is_matched):
        return (
            reference_institutes.loc[is_matched, institute_attributes]
            .iloc[0]
            .dropna()
            .to_dict()
        )

    is_ices_code = reference_institutes["ices_edmo_code"] == ices_code
    is_institute = (
        reference_institutes["dfo_code"].str.contains(institution) == True
    ) | (reference_institutes["institution"].str.contains(institution) == True)

    if is_ices_code.any():
        return _get_institute(is_ices_code)
    elif is_institute.any():
        return _get_institute(is_institute)
    else:
        logger.warn(f"Unknown ices_code {ices_code} and institution {institution}")
        return {}


def match_platform(platform):
    """Review ODF CRUISE_HEADER:PLATFORM"""
    platform = re.sub("CCGS\s*|CGCB\s*", "", platform)
    is_vessel = reference_vessel["platform"].str.lower().str.contains(platform.lower())
    if is_vessel.any():
        return (
            reference_vessel.loc[is_vessel, platform_attributes]
            .iloc[0]
            .dropna()
            .to_dict()
        )
    else:
        logger.warning(f"Unknown platform {platform}")
        return {}


def global_attributes_from_header(odf_header):
    """
    Method use to define the standard global attributes from an ODF Header parsed by the read function.
    """

    def _reviewLat(value):
        return None if value == -99.9 else value

    def _reviewLon(value):
        return None if value == -999.9 else value

    odf_original_header = odf_header.copy()
    odf_original_header.pop("variable_attributes")
    global_attributes = {
        "organization": "Fisheries and Oceans Canada (DFO)",
        "institution": odf_header["CRUISE_HEADER"]["ORGANIZATION"],
        "program": odf_header["CRUISE_HEADER"]["CRUISE_DESCRIPTION"],
        "project": odf_header["CRUISE_HEADER"]["CRUISE_DESCRIPTION"],
        "cruise_name": odf_header["CRUISE_HEADER"]["CRUISE_NAME"],
        "cruise_number": odf_header["CRUISE_HEADER"]["CRUISE_NUMBER"],
        "cruise_description": odf_header["CRUISE_HEADER"]["CRUISE_DESCRIPTION"],
        "chief_scientist": odf_header["CRUISE_HEADER"]["CHIEF_SCIENTIST"],
        "mission_start_date": odf_header["CRUISE_HEADER"].get("START_DATE"),
        "mission_end_date": odf_header["CRUISE_HEADER"].get("END_DATE"),
        "platform": odf_header["CRUISE_HEADER"]["PLATFORM"],
        "id": "",
        "naming_authority": "",
        "event_number": odf_header["EVENT_HEADER"]["EVENT_NUMBER"],
        "event_start_time": odf_header["EVENT_HEADER"]["START_DATE_TIME"],
        "event_end_time": odf_header["EVENT_HEADER"]["END_DATE_TIME"],
        "initial_latitude": _reviewLat(odf_header["EVENT_HEADER"]["INITIAL_LATITUDE"]),
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

    # Map COUNTRY_INSTITUTE_CODE to Institute Reference
    global_attributes.update(
        match_institute(
            odf_header["CRUISE_HEADER"]["COUNTRY_INSTITUTE_CODE"],
            odf_header["CRUISE_HEADER"]["ORGANIZATION"],
        )
    )

    # Map PLATFORM to NERC C17
    global_attributes.update(match_platform(odf_header["CRUISE_HEADER"]["PLATFORM"]))

    # Convert ODF history to CF history
    is_manufacturer_header = False
    global_attributes["instrument_manufacturer_header"] = ""
    global_attributes['internal_processing_notes'] = ""
    global_attributes['seabird_processing_modules'] = ""
    for history_group in odf_header["HISTORY_HEADER"]:
        # Convert single processes to list
        if type(history_group['PROCESS']) is str:
            history_group['PROCESS'] = [history_group['PROCESS']]

        for row in history_group["PROCESS"]:
            # Retrieve Instrument Manufacturer Header
            if row.startswith("* Sea-Bird"):
                global_attributes["history"] += "# SEA-BIRD INSTRUMENTS HEADER\n"
                is_manufacturer_header = True
            if is_manufacturer_header:
                global_attributes["instrument_manufacturer_header"] += row + "\n"
            else:
                global_attributes['internal_processing_notes'] += row + "\n"
            
            # End of manufacturer header
            if row.startswith("*END*"):
                is_manufacturer_header = False
                global_attributes["history"] += "# ODF Internal Processing Notes\n"

            # Ignore some specific lines within the history (mostly seabird header ones)
            if re.match("^(\#\s*\<.*|\*\* .*|\# (name|span|nquan|nvalues|unit|interval|start_time|bad_flag)|\* |\*END\*)", row):
                continue
            # Add to history    
            global_attributes["history"] += history_input(
                row, history_group["CREATION_DATE"]
            )

    # Instrument Specific Information
    if global_attributes["instrument_manufacturer_header"]:
        global_attributes['instrument'] = get_seabird_instrument_from_header(global_attributes["instrument_manufacturer_header"])
    elif "INSTRUMENT_HEADER" in odf_header:
        global_attributes['instrument'] =  odf_header["INSTRUMENT_HEADER"]["INST_TYPE"] + ' ' + odf_header["INSTRUMENT_HEADER"]["MODEL"]
    
    if re.search('(SBE\s*(9|16|19|25|37))', global_attributes['instrument']):
        global_attributes['instrument_type'] = "CTD"
    else:
        logging.warning(f"Unknown instrument type for {global_attributes['instrument']}")
        
    global_attributes["instrument_serial_number"] =  odf_header["INSTRUMENT_HEADER"]["SERIAL_NUMBER"]
    # TODO map instrument to seadatanet L22 instrument

    # Derive cdm_data_type from DATA_TYPE
    type_profile = {"CTD": "CTD", "BOTL": "Bottle", "BT": "Bottle"}
    data_type = odf_header["EVENT_HEADER"]["DATA_TYPE"]
    if data_type in ["CTD", "BOTL", "BT"]:
        global_attributes["cdm_data_type"] = "Profile"
        global_attributes["cdm_profile_variables"] = ""
        if data_type == "CTD":
            global_attributes["profile_direction"] = profile_direction_map[
                odf_header["EVENT_HEADER"]["EVENT_QUALIFIER2"]
            ]
        global_attributes["title"] = (
            f"{type_profile[data_type]} profile data collected "
            + f"from the {global_attributes['platform']} platform by "
            + f"{global_attributes['organization']}  {global_attributes['institution']} "
            + f"on the {global_attributes['cruise_name'].title()} "
            + f"from {pd.to_datetime(global_attributes['mission_start_date']).strftime('%d-%b-%Y')} "
            + f"to {global_attributes['mission_end_date'].strftime('%d-%b-%Y')}."
        )
    else:
        logger.error(
            f"Incompatible with ODF DATA_TYPE {odf_header['EVENT_HEADER']['DATA_TYPE']} yet."
        )

    # Search anywhere within ODF Header
    station = re.search(
        "\*\* Station_Name: (.*)',\n", "".join(odf_header["original_header"])
    )
    if station:
        global_attributes["station"] = station[1]

    # Missing terms potentially, mooring_number, station,
    return global_attributes


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
        "platform": {"ioos_category": "Other"},
        "event_number": {"ioos_category": "Other"},
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

            # Ignore empty keys
            if key not in ds.attrs or ds.attrs[key] in (None, pd.NaT):
                continue

            ds[new_key] = ds.attrs[key]
            ds[new_key].attrs = att

    # Reorder variables
    variable_list = [var for var in ds.keys() if var not in initial_variable_order]
    variable_list.extend(initial_variable_order)
    ds = ds[variable_list]
    return ds
