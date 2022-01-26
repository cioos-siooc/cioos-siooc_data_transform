"""
odf_parser is a module that regroup a different set of tools used to parse the ODF format which is use, maintain
and developped by the DFO offices BIO and MLI.
"""

import re
import warnings

import pandas as pd
import numpy as np
import xarray as xr

import json
import gsw


import logging

logger = logging.getLogger(__name__)

# Dictionary with the mapping of the odf types to python types
odf_dtypes = {
    "DOUB": "float64",
    "SING": "float32",
    "DOUBLE": "float64",
    "SYTM": str,
    "INTE": "int32",
    "CHAR": str,
    "QQQQ": "int32",
}

# Commonly date place holder used within the ODF files
flag_long_name_prefix = "Quality_Flag: "
original_prefix_var_attribute = "original_"


class GF3Code:
    """
    ODF GF3 Class split terms in their different components and standardize the convention (CODE_XX).  
    """

    def __init__(self, code):
        self.code = re.search("^[^_]*", code)[0]
        index = re.search("\d+$", code)
        if index:
            self.index = int(index[0])
        else:
            self.index = None
        self.name = self.code + ("_%02g" % int(self.index) if self.index else "")


def read(filename, encoding_format="Windows-1252"):
    """
    Read_odf
    Read_odf parse the odf format used by some DFO organisation to python list of dictionaryformat and
    pandas dataframe. Once converted, the output can easily be converted to netcdf format.

    Steps applied:
        1. Read line by line an ODF header and distribute each lines in a list of list and dictionaries.
            a. Lines associated with a character at the beginning are considered a section.
            b. Lines starting white spaces are considered items in preceding section.
            c. Repeated sections are grouped as a list
            d. Each section items are grouped as a dictionary
            e. dictionary items are converted to datetime (deactivated), string, integer or float format.
        2. Read the data  following the header with Pandas.read_csv() method
            a. Use defined separator  to distinguish columns (default "\s+").
            b. Convert each column of the pandas data frame to the matching format specified in
            the TYPE attribute of the ODF associated PARAMETER_HEADER

    read_odf is a simple tool that  parse the header metadata and data from an DFO ODF file to a list of dictionaries.
    :param filename: ODF file to read
    :param encoding_format: odf encoding format
     start of the data.
    :return:
    """

    def _convert_to_number(value):
        """Simple method to try to convert input (string, literals) to float or integer."""
        try:
            floated = float(value)
            if floated.is_integer():
                return int(floated)
            return floated
        except ValueError:
            return value

    metadata = {}  # Start with an empty dictionary
    with open(filename, encoding=encoding_format) as f:
        line = ""
        original_header = []
        # Read header one line at the time
        while "-- DATA --" not in line:
            line = f.readline()
            # Drop some characters that aren't useful
            line = re.sub(r"\n|,$", "", line)

            # Collect each original odf header lines
            original_header.append(line)

            # Sections
            if re.match(r"^\s?\w", line):
                section = line.replace("\n", "").replace(",", "")
                section = re.sub(
                    r"^\s*|\s$", "", section
                )  # Ignore white spaces before and after
                if section not in metadata:
                    metadata[section] = [{}]
                else:
                    metadata[section].append({})

            # Dictionary type lines (key=value)
            elif re.match(r"^\s{2}\s*\w", line):  # Something=This
                dict_line = re.split(
                    r"=", line, maxsplit=1
                )  # Make sure that only the first = is use to split
                dict_line = [
                    re.sub(r"^\s+|\s+$", "", item) for item in dict_line
                ]  # Remove trailing white spaces

                if re.match(
                    r"\'.*\'", dict_line[1]
                ):  # Is delimited by double quotes, definitely a string
                    # Drop the quote signs and the white spaces before and after
                    dict_line[1] = str(re.sub(r"^\s*|\s*$", "", dict_line[1][1:-1]))
                else:
                    # Try to convert the value of the dictionary in an integer or float
                    dict_line[1] = _convert_to_number(dict_line[1])

                # Add to the metadata as a dictionary
                key = dict_line[0].strip().replace(" ", "_")
                metadata[section][-1][key] = dict_line[1]

            else:
                assert RuntimeError, "Can't understand the line: " + line

        # Simplify the single sections to a dictionary
        for section in metadata:
            if len(metadata[section]) == 1 and type(metadata[section][0]) is dict:
                metadata[section] = metadata[section][0]

        # Add original header in text format to the dictionary
        metadata["original_header"] = original_header

        # READ PARAMETER_HEADER
        # Define first the variable name and attributes and the type.
        metadata["variable_attributes"] = {}
        time_columns = []
        # Variable names and related attributes
        for att in metadata["PARAMETER_HEADER"]:
            # Generate variable name
            if "CODE" in att:
                var_name = GF3Code(att["CODE"]).name
            elif (
                "NAME" in att
                and "WMO_CODE" in att
                and att["NAME"].startswith(att["WMO_CODE"])
            ):
                var_name = GF3Code(att["NAME"]).name
            else:
                raise RuntimeError("Unrecognizable ODF variable attributes")

            attribute = {
                "long_name": att.get("NAME"),
                "units": att.get("UNITS"),
                "gf3_code": var_name,
                "type": att["TYPE"],
                "null_value": att["NULL_VALUE"],
                "comments": "Original ODF Variable Attributes:\n"
                + json.dumps(
                    {"odf_variable_attributes": att}, ensure_ascii=False, indent=False
                ),
            }

            # Add those variable attributes to the metadata output
            metadata["variable_attributes"].update({var_name: attribute})
            # Time type column add to time variables to parse by pd.read_csv()
            if var_name.startswith("SYTM") or att["TYPE"] == "SYTM":
                time_columns.append(var_name)

        # If not time column replace by False
        if not time_columns:
            time_columns = False

        # Read Data with Pandas
        data_raw = pd.read_csv(
            f,
            delimiter=r"\s+",
            quotechar="'",
            header=None,
            names=metadata["variable_attributes"].keys(),
            dtype={
                key: odf_dtypes[att.pop("type")]
                for key, att in metadata["variable_attributes"].items()
            },
            na_values={
                key: att.pop("null_value")
                for key, att in metadata["variable_attributes"].items()
            },
            parse_dates=time_columns,
            encoding=encoding_format,
        )

    # Make sure that there's the same amount of variables read versus what is suggested in the header
    if len(data_raw.columns) != len(metadata["PARAMETER_HEADER"]):
        raise RuntimeError(
            "{0} variables were detected in the data versus {1} in the header.".format(
                len(data_raw.columns), len(metadata["PARAMETER_HEADER"])
            )
        )

    # Make sure that timezone is UTC, GMT or None
    # (This may not be necessary since we're looking at the units later now)
    if time_columns:
        for parm in time_columns:
            units = metadata["variable_attributes"][parm].get(
                original_prefix_var_attribute + "UNITS"
            )
            if units not in [None, "none", "(none)", "GMT", "UTC", "seconds"]:
                logger.warn(f"{filename}: {parm} has UNITS (timezone) of {units}")

    # TODO review if the count of flagged values versus good is the same as ODF attributes NUMBER_VALID NUMBER_NULL
    return metadata, data_raw


def odf_flag_variables(ds, flag_convention=None):
    """
    odf_flag_variables handle the different conventions used within the ODF files over the years and map them
     to the CF standards.
    """

    # Loop through each variables and detect flag variables
    previous_key = None
    for var in ds:
        attrs = ds[var].attrs
        related_variables = None

        # Find related variable
        if var.startswith("QQQQ"):
            # FLAG QQQQ should apply to previous variable
            related_variables = [previous_key]

            # Rename variable so that we can link by variable name
            ds = ds.rename({var: f"Q{previous_key}"})
            var = f"Q{previous_key}"

        elif var.startswith(("QCFF", "FFFF")):
            # This is a general flag
            related_variables = [var for var in ds if not var.startswith("Q")]

        elif var.startswith("Q") and var[1:] in ds:
            # Q  Format is usually Q+[PCODE] of the associated variable
            related_variables = [var[1:]]

        else:
            # If the variable isn't a flag variable, go to the next iteration
            # Set previous key for the next iteration
            previous_key = var
            continue

        # Add flag variable to related variable ancillary_variables attribute
        for related_variable in related_variables:
            if "ancillary_variables" in ds[related_variable].attrs:
                ds[related_variable].attrs["ancillary_variables"] += f" {var}"
            else:
                ds[related_variable].attrs["ancillary_variables"] = var

        # Add flag convention attributes if available within config file
        if flag_convention:
            if var in flag_convention:
                ds[var].attrs.update(flag_convention[var])
            elif "default" in flag_convention:
                ds[var].attrs.update(flag_convention["default"])

            # Match flag_values data type to variable data type
            ds[var].attrs["flag_values"] = tuple(
                np.array(ds[var].attrs["flag_values"]).astype(ds[var].dtype)
            )

        # Set previous key for the next iteration
        previous_key = var

    return ds


def get_vocabulary_attributes(ds, organizations=None, vocabulary=None):
    """
    This method is use to retrieve from an ODF variable code, units and units, matching vocabulary terms available.
    """

    def detect_reference_scale(variable_name, attributes):
        scales = {
            "IPTS-48": r"IPTS\-48",
            "IPTS-68": r"IPTS\-68|ITS\-68",
            "ITS-90": r"ITS\-90|TE90",
            "PSS-78": r"PSS\-78|practical.*salinity|psal",
        }
        for scale, scale_search in scales.items():
            if re.search(scale_search, variable_name, re.IGNORECASE):
                return scale

            for key, value in attributes.items():
                if type(value) is str and re.search(scale_search, value, re.IGNORECASE):
                    return scale
        return None

    def _review_term(term, accepted_terms, regexp=False, search_flag=None):
        """
        Simple tool to compare "|" separated units in the Vocabulary expected unit list.
        - First unit if any is matching.
        - True if empty or expected to be empty
        - unknown if unit exists but the "accepted_units" input is empty.
        - False if not matching units
        """
        if accepted_terms == None:
            # No required term
            return True
        elif re.search("^none$|^dimensionless$", accepted_terms, re.IGNORECASE):
            # Truly expect no units
            return True
        elif term in accepted_terms.split("|"):
            # Match exactely one of the listed terms
            return True
        elif regexp and re.search(accepted_terms, term, search_flag):
            # Match expected term
            return True
        else:
            # term do not match expected terms
            return False

    # Define vocabulary default list of variables to import as attributes
    vocabulary_attribute_list = [
        "long_name",
        "units",
        "instrument",
        "scale",
        "standard_name",
        "sdn_parameter_urn",
        "sdn_parameter_name",
        "sdn_uom_urn",
        "sdn_uom_name",
        "definition",
        "convention",
    ]

    # Generate Standardized Attributes from vocabulary table
    # # The very first item in the expected columns is the main term to use
    # vocabulary["units"] = vocabulary["accepted_units"].str.split("|").str[0]
    # vocabulary["instrument"] = vocabulary["accepted_instrument"].str.split("|").str[0]

    # Find matching vocabulary
    variable_list = [variable for variable in ds]
    new_variable_order = []
    for var in variable_list:
        attrs = ds[var].attrs

        # Ignore variables with no attributes and flag variables
        if attrs == {} or attrs.get("flag_values"):
            new_variable_order.append(var)
            continue

        # Retrieve standardize units and scale
        var_units = standardize_odf_units(attrs.get("units", "none"))
        scale = detect_reference_scale(var, attrs)
        if scale:
            # Add scale to to scale attribute
            attrs["scale"] = scale

        # Find matching vocabulary for that GF3 Code (if available) or variable name
        if "gf3_code" in attrs:
            gf3 = GF3Code(attrs["gf3_code"])
            name = gf3.code
        else:
            gf3 = None
            name = var
        matching_terms = vocabulary.query(
            f"Vocabulary == {organizations} and name=='{name}'"
        )

        # If nothing matches, move to the next one
        if matching_terms.empty:
            new_variable_order.append(var)
            continue

        # Consider only the first organization that has this term
        selected_organization = matching_terms.index[0][0]
        matching_terms = matching_terms.loc[matching_terms.index[0][0]]

        # Among these matching terms find matching ones
        match_units = matching_terms["accepted_units"].apply(
            lambda x: _review_term(var_units, x)
        )
        match_scale = matching_terms["accepted_scale"].apply(
            lambda x: _review_term(attrs.get("scale"), x)
        )
        match_instrument = matching_terms["accepted_instruments"].apply(
            lambda x: _review_term(
                attrs["long_name"], x, regexp=True, search_flag=re.IGNORECASE
            )
        )
        instrument_name = (
            f"{ds.attrs.get('instrument_type')} {ds.attrs.get('instrument_model')}"
        )
        match_instrument_global = matching_terms["accepted_instruments"].apply(
            lambda x: _review_term(
                instrument_name, x, regexp=True, search_flag=re.IGNORECASE
            )
        )

        # Select only the terms that matches all the units/scale/instrument conditions
        matching_terms_and_units = matching_terms.loc[
            match_units & match_scale & (match_instrument | match_instrument_global)
        ]

        # No matching term, give a warning if not a flag and move on to the next iteration
        if len(matching_terms_and_units) == 0:
            logger.warn(
                f"ODF File: {ds['file_id'].values} -> No Matching unit found for code: {var}, long_name: {attrs.get('long_name')}, units: {attrs.get('units')} in vocabulary {selected_organization}\n"
                + f"{matching_terms[['accepted_units','accepted_scale','accepted_instruments']]}"
            )
            new_variable_order.append(var)
            continue

        # Generate new variables and update original variable attributes from vocabulary
        new_variable_order.append(var)
        for index, row in matching_terms_and_units.iterrows():
            # Make a copy of original variable
            if row["variable_name"]:
                # Apply suffix number of original variable
                if gf3 and 1 < gf3.index < 10:
                    new_variable = re.sub("1$", "%1g" % gf3.index, row["variable_name"])
                else:
                    new_variable = row["variable_name"]

                if new_variable in ds:
                    # If the new variable already exist in the dataset continue
                    continue

                # Generate new variable by either copying it or applying specified function to the initial variable
                if row["apply_function"]:
                    ds[new_variable] = xr.apply_ufunc(
                        eval(row["apply_function"]), (ds[var]), keep_attrs=True
                    )
                else:
                    ds[new_variable] = ds[var].copy()

                new_attrs = ds[new_variable].attrs
                new_variable_order.append(new_variable)
            else:
                # Apply vocabulary to original variable
                new_attrs = ds[var].attrs

            # Retrieve all attributes in vocabulary that have something
            new_attrs.update(row[vocabulary_attribute_list].dropna().to_dict())

            # If original data has units but vocabulary doesn't require one drop the units
            if "units" in new_attrs and row["units"] is None:
                new_attrs.pop("units")

            # Update sdn_parameter_urn term available to match trailing number from the variable itself.
            if "sdn_parameter_urn" in new_attrs and "gf3_code" in new_attrs:
                new_attrs["sdn_parameter_urn"] = re.sub(
                    r"\d\d|XX$", "%02d" % gf3.index, new_attrs["sdn_parameter_urn"],
                )
    return ds[new_variable_order]


def standardize_odf_units(unit_string):
    """
    Units strings were manually written within the ODF files.
    We're trying to standardize all the different issues found.
    """
    if unit_string:
        unit_string = unit_string.replace("**", "^")
        unit_string = unit_string.replace("µ", "u")
        unit_string = re.sub(r" /|/ ", "/", unit_string)
        unit_string = re.sub(r" \^|\^ ", "^", unit_string)

        if re.match(r"\(none\)|none|dimensionless", unit_string):
            unit_string = "none"
    return unit_string


def global_attributes_from_header(odf_header):
    """
    Method use to define the standard global attributes from an ODF Header parsed by the read function.
    """
    odf_original_header = odf_header.copy()
    odf_original_header.pop("variable_attributes")
    global_attributes = {
        "project": odf_header["CRUISE_HEADER"]["CRUISE_NAME"],
        "institution": odf_header["CRUISE_HEADER"]["ORGANIZATION"],
        "country_code": odf_header["CRUISE_HEADER"]["COUNTRY_INSTITUTE_CODE"],
        "cruise_number": odf_header["CRUISE_HEADER"]["CRUISE_NUMBER"],
        "cruise_name": odf_header["CRUISE_HEADER"]["CRUISE_NAME"],
        "cruise_description": odf_header["CRUISE_HEADER"]["CRUISE_DESCRIPTION"],
        "scientist": odf_header["CRUISE_HEADER"]["CHIEF_SCIENTIST"],
        "platform": odf_header["CRUISE_HEADER"]["PLATFORM"],
        "data_type": odf_header["CRUISE_HEADER"].get("DATA_TYPE", ""),
        "sampling_interval": odf_header["EVENT_HEADER"]["SAMPLING_INTERVAL"],
        "water_depth": odf_header["EVENT_HEADER"]["SOUNDING"],
        "date_created": odf_header["EVENT_HEADER"]["ORIG_CREATION_DATE"],
        "date_modified": odf_header["EVENT_HEADER"]["CREATION_DATE"],
        "history": json.dumps(
            odf_header["HISTORY_HEADER"], ensure_ascii=False, indent=False
        ),
        "comment": odf_header["EVENT_HEADER"].get("EVENT_COMMENTS", ""),
        "original_odf_header": "\n".join(odf_header["original_header"]),
        "original_odf_header_json": json.dumps(
            odf_original_header, ensure_ascii=False, indent=False
        ),
    }

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
    # Missing terms potentially, mooring_number, station,
    return global_attributes


def convert_odf_time(time_string):
    """Simple tool to convert ODF timestamps to a datetime object"""
    if time_string == "17-NOV-1858 00:00:00.00":
        return None
    else:
        return pd.to_datetime(time_string, utc=True)


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

    # Time Variables
    if odf_header["EVENT_HEADER"]["START_DATE_TIME"]:
        ds["start_time"] = convert_odf_time(
            odf_header["EVENT_HEADER"]["START_DATE_TIME"]
        )
        ds["start_time"].attrs = {
            "original_var_field": "EVENT_HEADER:START_DATE_TIME",
            "long_name": "Event Start Time",
        }

    if convert_odf_time(odf_header["EVENT_HEADER"]["END_DATE_TIME"]):
        ds["end_time"] = convert_odf_time(odf_header["EVENT_HEADER"]["END_DATE_TIME"])
        ds["end_time"].attrs = {
            "original_var_field": "EVENT_HEADER:END_DATE_TIME",
            "long_name": "Event Start Time",
        }

    # Time is handled differently for profile variables since there's not always a time variable within the ODF.
    # Time series and trajectory data should both have a time variable in the ODF.
    if cdm_data_type == "Profile":
        # Time variable
        if "start_time" in ds and ds["start_time"].item():
            ds.coords["time"] = ds["start_time"].copy()
        # elif "SYTM_01" in ds.keys():
        #     ds.coords["time"] = ds["SYTM_01"].min().values
        #     ds["time"].attrs[original_var_field] = "min(SYMT_01)"

        # precise time
        if "SYTM_01" in ds.keys():
            ds["measurement_time"] = ds["SYTM_01"].copy()
    else:
        ds.coords["time"] = ds["SYTM_01"]
        ds["time"].attrs[original_var_field] = "SYTM_01"

    # Add standard_name attribute
    ds["time"].attrs["standard_name"] = "time"

    # Coordinate variables
    # Latitude (ODF uses a place holder -99 in some of their header for latitude)
    initial_latitude = odf_header["EVENT_HEADER"].get("INITIAL_LATITUDE", -99)
    has_latitude_timeseries = "LATD_01" in ds

    if cdm_data_type in ["Profile", "TimeSeries"]:
        # Let's define latitude first, use start latitude if available
        if initial_latitude != -99:
            ds.coords["latitude"] = initial_latitude
            ds["latitude"].attrs[original_var_field] = "EVENT_HEADER:INITIAL_LATITUDE"

        # If a latitude time series is available, copy it to a preciseLat variable as suggested by ERDDAP
        if has_latitude_timeseries:
            ds["preciseLat"] = ds["LATD_01"].copy()
            ds["preciseLat"].attrs[original_var_field] = "LATD_01"
            ds["preciseLat"].attrs.update(
                {"units": "degrees_north", "standard_name": "latitude"}
            )
    elif has_latitude_timeseries:
        ds.coords["latitude"] = ds["LATD_01"]
        ds["latitude"].attrs[original_var_field] = "LATD_01"

    # Make sure there's a latitude
    if "latitude" not in ds:
        raise RuntimeError("Missing Latitude input")

    # Add latitude attributes
    ds["latitude"].attrs.update(
        {"long_name": "Latitude", "units": "degrees_north", "standard_name": "latitude"}
    )

    # Longitude (ODF uses a place holder -999 in some of their header for longitude)
    initial_longitude = odf_header["EVENT_HEADER"].get("INITIAL_LONGITUDE", -999)
    has_longitude_time_series = "LOND_01" in ds

    if cdm_data_type in ["Profile", "TimeSeries"]:
        if initial_longitude != -999:
            ds.coords["longitude"] = initial_longitude
            ds["longitude"].attrs[original_var_field] = "EVENT_HEADER:INITIAL_LONGITUDE"

        # If a longitude time series is available, copy it to a preciseLat variable as suggested by ERDDAP
        if has_longitude_time_series:
            ds["preciseLon"] = ds["LOND_01"].copy()
            ds["preciseLon"].attrs[original_var_field] = "LOND_01"
            ds["preciseLon"].attrs.update(
                {"units": "degrees_east", "standard_name": "longitude"}
            )
    elif has_longitude_time_series:
        ds.coords["longitude"] = ds["LOND_01"]
        ds["longitude"].attrs[original_var_field] = "LOND_01"

    # Make sure there's a longitude
    if "longitude" not in ds:
        raise RuntimeError("Missing Longitude input")
    # Add longitude attributes
    ds["longitude"].attrs.update(
        {
            "long_name": "Longitude",
            "units": "degrees_east",
            "standard_name": "longitude",
        }
    )

    # Depth
    if "DEPH_01" in ds:
        ds.coords["depth"] = ds["DEPH_01"]
        ds["depth"].attrs[original_var_field] = "DEPH_01"
    elif "PRES_01" in ds:
        ds.coords["depth"] = (
            ds["PRES_01"].dims,
            -gsw.z_from_p(ds["PRES_01"].data, ds["latitude"].data),
        )
        ds["depth"].attrs[original_var_field] = "-gsw.z_from_p(PRES_01,latitude)"
    elif (
        "MIN_DEPTH" in odf_header["EVENT_HEADER"]
        and "MAX_DEPTH" in odf_header["EVENT_HEADER"]
        and odf_header["EVENT_HEADER"]["MAX_DEPTH"]
        - odf_header["EVENT_HEADER"]["MIN_DEPTH"]
        == 0
    ):
        ds.coords["depth"] = odf_header["EVENT_HEADER"]["MAX_DEPTH"]
        ds["depth"].attrs[original_var_field] = "EVENT_HEADER:MIN|MAX_DEPTH"
    else:
        # If no depth variable is available we'll assume it's not suppose to happen for now.
        raise RuntimeError("Missing a depth information")

    if "depth" in ds:
        ds["depth"].attrs.update(
            {
                "units": "m",
                "standard_name": "depth",
                "positive": "down",
                "long_name": "Depth",
            }
        )

    # Reorder variables
    variable_list = [var for var in ds.keys() if var not in initial_variable_order]
    variable_list.extend(list(ds.coords.keys()))
    variable_list.extend(initial_variable_order)
    ds = ds[variable_list]
    return ds
