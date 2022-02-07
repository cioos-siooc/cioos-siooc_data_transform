import pandas as pd
import re
import json
from .parser import convert_odf_time


def titleize(text):
    do_not_change = ["AZMP", "(AZMP)", "ADCP", "(ADCP)", "CTD", "a", "the"]
    return " ".join(
        item.title() if item not in do_not_change else item for item in text.split(" ")
    )


def global_attributes_from_header(odf_header):
    """
    Method use to define the standard global attributes from an ODF Header parsed by the read function.
    """

    odf_original_header = odf_header.copy()
    odf_original_header.pop("variable_attributes")
    global_attributes = {
        "institution": odf_header["CRUISE_HEADER"]["ORGANIZATION"],
        "country_code": odf_header["CRUISE_HEADER"]["COUNTRY_INSTITUTE_CODE"],
        "project": odf_header["CRUISE_HEADER"]["CRUISE_NAME"],
        "cruise_number": odf_header["CRUISE_HEADER"]["CRUISE_NUMBER"],
        "cruise_name": odf_header["CRUISE_HEADER"]["CRUISE_NAME"],
        "cruise_description": odf_header["CRUISE_HEADER"]["CRUISE_DESCRIPTION"],
        "scientist": odf_header["CRUISE_HEADER"]["CHIEF_SCIENTIST"],
        "platform": odf_header["CRUISE_HEADER"]["PLATFORM"],
        "data_type": odf_header["CRUISE_HEADER"].get("DATA_TYPE", ""),
        "event_number": odf_header["EVENT_HEADER"]["EVENT_NUMBER"],
        "event_qualifier1": odf_header["EVENT_HEADER"]["EVENT_QUALIFIER1"],
        "event_qualifier2": odf_header["EVENT_HEADER"]["EVENT_QUALIFIER2"],
        "sampling_interval": odf_header["EVENT_HEADER"]["SAMPLING_INTERVAL"],
        "water_depth": odf_header["EVENT_HEADER"]["SOUNDING"],
        "date_created": odf_header["EVENT_HEADER"]["ORIG_CREATION_DATE"],
        "date_modified": odf_header["EVENT_HEADER"]["CREATION_DATE"],
        "history": "",
        "comment": odf_header["EVENT_HEADER"].get("EVENT_COMMENTS", ""),
        "original_odf_header": "\n".join(odf_header["original_header"]),
        "original_odf_header_json": json.dumps(
            odf_original_header, ensure_ascii=False, indent=False
        ),
    }

    # Convert ODF history to CF history
    for history_group in odf_header["HISTORY_HEADER"]:
        date = convert_odf_time(history_group["CREATION_DATE"], str)
        for row in history_group["PROCESS"]:
            global_attributes["history"] += f"{date} {row}\n"

    # Instrument Specific Information
    if "INSTRUMENT_HEADER" in odf_header:
        global_attributes.update(
            {
                "instrument_type": odf_header["INSTRUMENT_HEADER"]["INST_TYPE"],
                "instrument_model": odf_header["INSTRUMENT_HEADER"]["MODEL"],
                "instrument_serial_number": odf_header["INSTRUMENT_HEADER"][
                    "SERIAL_NUMBER"
                ],
                "instrument_description": odf_header["INSTRUMENT_HEADER"][
                    "DESCRIPTION"
                ],
            }
        )
        # Retrieve Seabird Format Instruments Calibration from history
        calibration = re.findall("\#(\s*\<.*)", global_attributes["history"])
        if calibration:
            global_attributes["instrument_calibration"] = calibration

    # Derive cdm_data_type from DATA_TYPE
    if odf_header["EVENT_HEADER"]["DATA_TYPE"] in ["CTD", "BOTL"]:
        global_attributes["cdm_data_type"] = "Profile"
        global_attributes["cdm_profile_variable_type"] = ""
        global_attributes["profile_direction"] = odf_header["EVENT_HEADER"][
            "EVENT_QUALIFIER2"
        ]
    else:
        raise RuntimeError(
            f"Incompatible with ODF DATA_TYPE {odf_header['EVENT_HEADER']['DATA_TYPE']} yet."
        )

    # Missing terms potentially, mooring_number, station,
    return global_attributes



def generate_variables_from_header(
    ds, odf_header, cdm_data_type, original_var_field="source"
):
    """
    Method use to generate metadata variables from the ODF Header to a xarray Dataset.
    """
    initial_variable_order = list(ds.keys())

    # General Attributes
    ds["institution"] = odf_header["CRUISE_HEADER"]["ORGANIZATION"]
    ds["cruise_name"] = odf_header["CRUISE_HEADER"]["CRUISE_NAME"]
    ds["cruise_id"] = odf_header["CRUISE_HEADER"].get("CRUISE_NUMBER", "")
    ds["chief_scientist"] = odf_header["CRUISE_HEADER"]["CHIEF_SCIENTIST"]
    ds["platform"] = odf_header["CRUISE_HEADER"]["PLATFORM"]

    ds["event_number"] = odf_header["EVENT_HEADER"]["EVENT_NUMBER"]
    ds["event_qualifier1"] = odf_header["EVENT_HEADER"]["EVENT_QUALIFIER1"]

    ds["start_time"] = convert_odf_time(odf_header["EVENT_HEADER"]["START_DATE_TIME"])
    ds["start_time"].attrs = {"source": "EVENT_HEADER:START_DATE_TIME"}
    ds["end_time"] = convert_odf_time(odf_header["EVENT_HEADER"]["END_DATE_TIME"])
    ds["end_time"].attrs = {"source": "EVENT_HEADER:END_DATE_TIME"}

    ds["initial_latitude"] = odf_header["EVENT_HEADER"]["INITIAL_LATITUDE"]
    ds["initial_latitude"].attrs = {
        "units": "degrees_north",
        "source": "EVENT_HEADER:INITIAL_LATITUDE",
    }
    ds["initial_longitude"] = odf_header["EVENT_HEADER"]["INITIAL_LONGITUDE"]
    ds["initial_longitude"].attrs = {
        "units": "degrees_east",
        "source": "EVENT_HEADER:INITIAL_LONGITUDE",
    }

    if ds.attrs["cdm_data_type"] == "Profile":
        # Define profile specific variables
        ds["profile_id"] = odf_header["ODF_HEADER"]["FILE_SPECIFICATION"]
        ds["profile_id"].attrs = {"cf_role": "profile_id"}
        ds["profile_direction"] = odf_header["EVENT_HEADER"]["EVENT_QUALIFIER2"]
        ds["latitude"] = ds["initial_latitude"]
        ds["longitude"] = ds["initial_longitude"]
        ds["latitude"].attrs["standard_name"] = "latitude"
        ds["longitude"].attrs["standard_name"] = "longitude"

        ds["time"] = ds["start_time"]
        ds["time"].attrs["standard_name"] = "time"

    # Reorder variables
    variable_list = [var for var in ds.keys() if var not in initial_variable_order]
    variable_list.extend(initial_variable_order)
    ds = ds[variable_list]
    return ds
