"""
odf_parser is a module that regroup a different set of tools used to parse the ODF format which is use, maintain
and developped by the DFO offices BIO and MLI.
"""

import re
import datetime as dt
import warnings
import pandas as pd
import json
import gsw

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
                key = dict_line[0].strip().replace(' ', '_')
                metadata[section][-1][key] = dict_line[1]

            else:
                assert RuntimeError, "Can't understand the line: " + line

        # Simplify the single sections to a dictionary
        for section in metadata:
            if len(metadata[section]) == 1 and type(metadata[section][0]) is dict:
                metadata[section] = metadata[section][0]

        # Add original header in text format to the dictionary
        metadata['original_header'] = original_header

        # READ PARAMETER_HEADER
        # Define first the variable name and attributes and the type.
        metadata["variable_attributes"] = {}
        time_columns = []
        # Variable names and related attributes
        for att in metadata["PARAMETER_HEADER"]:
            # Generate variable name
            if "CODE" in att:
                var_name = parse_odf_code_variable(att["CODE"])
            elif (
                    "NAME" in att
                    and "WMO_CODE" in att
                    and att["NAME"].startswith(att["WMO_CODE"])
            ):
                var_name = parse_odf_code_variable(att["NAME"])
            else:
                raise RuntimeError("Unrecognizable ODF variable attributes")

            attribute = {
                "long_name": att.get("NAME"),
                "units": att.get("UNITS"),
                "gf3_code": var_name["standardized_name"],
                "type": att["TYPE"],
                "null_value": att["NULL_VALUE"],
                "comments": json.dumps(
                    {"odf_variable_attributes": att}, ensure_ascii=False, indent=False
                ),
            }
            # Generate variable attributes
            output_variable_name = var_name["standardized_name"]

            # Add those variable attributes to the metadata output
            metadata["variable_attributes"].update({output_variable_name: attribute})
            # Time type column add to time variables to parse by pd.read_csv()
            if output_variable_name.startswith("SYTM") or att["TYPE"] == "SYTM":
                time_columns.append(output_variable_name)

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
                warnings.warn(
                    "{0} has UNITS(timezone) of {1}".format(parm, units), UserWarning
                )

    # TODO review if the count of flagged values versus good is the same as ODF attributes NUMBER_VALID NUMBER_NULL
    return metadata, data_raw


def odf_flag_variables(metadata, flag_convention=None):
    """
    odf_flag_variables handle the different conventions used within the ODF files over the years and map them
     to the CF standards.
    """

    # Loop through each variables and detect flag variables
    previous_key = None
    for var, att in metadata.items():
        # Retrieve information from variable name
        odf_var_name = parse_odf_code_variable(var)
        related_variable = None

        # FLAG VARIABLES Detect if it is a flag column
        is_q_flag = var.startswith("Q") and var[1:] in metadata.keys()
        is_qqqq_flag = odf_var_name["name"] == "QQQQ"
        is_general_flag = odf_var_name["name"] in ["QCFF", "FFFF"]

        is_flag_column = is_q_flag or is_qqqq_flag or is_general_flag

        # Find related variable
        if is_qqqq_flag:
            # FLAG QQQQ should apply to previous variable
            related_variable = previous_key
        if is_q_flag:
            # Q  Format is usually Q+[PCODE] of the associated variable
            related_variable = var[1:]

        # If the variable isn't a flag variable, go to the next iteration
        if not is_flag_column:
            # Set previous key for the next iteration
            previous_key = var
            continue

        # If flag is specific to a variable, try to confirm if the odf name of the related variable match the
        # flag name. If yes, add this flag variable to the ancillary_attribute of the related variable
        if related_variable:
            # Make sure that the related variable exist
            if related_variable not in metadata:
                raise KeyError(
                    "{0} flag is referring to {1} which is not available as variable".format(
                        var, related_variable
                    ),
                )

            # Try to see if the related_variable and flag have matching name or code in odf
            related_variable_name = re.sub(
                r"quality\sflag.*:\s*|quality flag of ",
                "",
                att["long_name"],
                1,
                re.IGNORECASE,
            )
            # Flag name do not match either variable name or code, give a warning.
            if related_variable_name not in metadata[related_variable].get(
                    "long_name"
            ) and related_variable_name not in metadata[related_variable].get(
                "gf3_code"
            ):
                warnings.warn(
                    "{0}[{1}] flag was matched to the variable {2} but ODF attributes {3} do not match".format(
                        var,
                        att["long_name"],
                        related_variable,
                        {
                            key: metadata[related_variable].get(key)
                            for key in ["long_name", "gf3_code"]
                        },
                    ),
                    UserWarning,
                )
            # Rename QQQQ Flag variables to the Q* standard
            if is_qqqq_flag:
                rename_var = 'Q' + related_variable
                metadata[var]['name'] = rename_var
            else:
                rename_var = var

            # Standardize long name attribute of flag variables
            if "name" in metadata[related_variable]:
                att["long_name"] = (
                        flag_long_name_prefix + metadata[related_variable]["name"]
                )
            else:
                att["long_name"] = flag_long_name_prefix + related_variable

            # Add ancillary_variables attribute
            if "ancillary_variables" in metadata[related_variable]:
                metadata[related_variable]["ancillary_variables"] += ",{0}".format(rename_var)
            else:
                metadata[related_variable]["ancillary_variables"] = rename_var

        # Add flag convention attributes if available within config file
        if flag_convention:
            if var in flag_convention:
                att.update(flag_convention[var])
            elif "default" in flag_convention:
                att.update(flag_convention["default"])

        # Set previous key for the next iteration
        previous_key = var

        # TODO rename QQQQ_XX flag variables to Q[related_variables] so that ERDDAP can easily amalgamate them!
    return metadata


def get_vocabulary_attributes(
        metadata, organizations=None, vocabulary=None, vocabulary_attribute_list=None, global_attributes=None
):
    """
    This method is use to retrieve from an ODF variable code, units and units, matching vocabulary terms available.
    """

    def detect_reference_scale(variable_name, attributes):
        scales = {
            "Flag": r"Quality.*Flag|Flag",
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

    def _compare_units(unit, expected_units, search=False, search_flag=None):
        """
        Simple tool to compare "|" separated units in the Vocabulary expected unit list.
        - First unit if any is matching.
        - True if empty or expected to be empty
        - unknown if unit exists but the "expected_units" input is empty.
        - False if not matching units
        """
        none_units = ["none", "dimensionless"]
        if expected_units:
            standardized_term = expected_units.split('|')[0]
            if standardized_term in none_units:
                standardized_term = None
        else:
            standardized_term = None

        if expected_units is None or any([True for item in none_units if item in expected_units]):
            accept_none = True
        else:
            accept_none = False

        # If a matching unit is found, return the standard unit (first one listed)
        if unit and expected_units and (re.match(expected_units, unit) or unit in expected_units.split('|')):
            return standardized_term
        elif unit and expected_units and search and re.search(expected_units, unit, search_flag):
            return True
        # If unit is None and expected term accept None return standardized unit
        elif unit in [None, ''] and accept_none:
            return standardized_term
        # If unit is None and expected term accept None return None
        elif (unit is None or unit in none_units) and accept_none:
            return None
        # If there's a Unit and standard unit is None, give back unknown. We don't what it's suppose to be.
        elif unit and expected_units is None:
            return "unknown"
        # If unit doesn't match the list return False
        else:
            return False

    # Define vocabulary default list of variables to import as attributes
    if vocabulary_attribute_list is None:
        vocabulary_attribute_list = (
            "standard_name",
            "sdn_parameter_urn",
            "sdn_parameter_name",
            "sdn_uom_urn",
            "sdn_uom_name",
        )

    # Find matching vocabulary
    new_variables = {}
    for var, attributes in metadata.items():
        # Separate the parameter_code from the number at the end of the variable
        parameter_code = parse_odf_code_variable(var)

        # We already standardized all the flags with the standard long_name attribute. Let's detect them with that.
        flag_column = attributes.get("long_name", "").startswith(
            flag_long_name_prefix
        ) or parameter_code["name"] in ["QCFF", "FFFF"]

        # Get ODF variable units and make some simple standardization
        var_units = standardize_odf_units(attributes.get("units", "none"))

        # Detect reference scale if present
        scale = detect_reference_scale(var, attributes)
        if scale:
            attributes['scale'] = scale

        # Find matching vocabularies and code and sort by given vocabularies
        matching_terms = vocabulary[
            vocabulary.index.isin(organizations, level=0)
            & vocabulary.index.isin([parameter_code["name"]], level=1)
            ].copy()

        # Among these matching terms find matching units
        matching_terms["standardize_units"] = matching_terms["expected_units"].apply(
            lambda x: _compare_units(var_units, x)
        )

        # Matching reference_scale
        matching_terms["matching_scale"] = matching_terms["expected_scale"].apply(
            lambda x: _compare_units(attributes.get('scale'), x))

        # Matching instrument
        # Add Variable level
        matching_terms["matching_instrument"] = matching_terms["expected_instrument"].apply(
            lambda x: _compare_units(attributes['long_name'], x, search=True, search_flag=re.IGNORECASE)
        )

        # At global attribute level
        instrument_name = f"{global_attributes.get('instrument_type')} {global_attributes.get('instrument_model')}"
        matching_terms["matching_instrument_global"] = matching_terms["expected_instrument"].apply(
            lambda x: _compare_units(instrument_name, x, search=True, search_flag=re.IGNORECASE)
        )

        # Drop the terms with no matching units (=False)
        matching_terms_and_units = matching_terms.loc[
            ~matching_terms["standardize_units"].isin([False]) &
            ~matching_terms["matching_scale"].isin([False]) &
            (
                    ~matching_terms["matching_instrument"].isin([False]) |
                    ~matching_terms["matching_instrument_global"].isin([False])
            )
            ].copy()

        if var in ['TEMP_01', 'PSAL_01', 'TE90_01'] and (
                (matching_terms['matching_instrument_global'].isin([False, 'unknown'])).all() and
                (matching_terms['matching_instrument'].isin([False, 'unknown'])).all()):
            print('unknown sensor')

        # No matching term, give a warning if not a flag and move on to the next iteration
        if len(matching_terms_and_units) == 0:
            if flag_column:
                continue
            elif len(matching_terms) == 0:
                warnings.warn(
                    f"{parameter_code['name']}[{attributes['units']}](ODF CODE: {var}) is " \
                        f"not available in vocabularies: {organizations}"
                    ,
                    UserWarning,
                )
            elif len(matching_terms_and_units) == 0:
                # If it has found a matching terms but not unit.
                warnings.warn(
                    "No Matching unit found for {0} [{1}] in: {2}".format(
                        var,
                        attributes.get("units"),
                        matching_terms["expected_units"].to_dict(),
                    ),
                    UserWarning,
                )
            continue

        # Replace empty variable_names by None and group matching terms by output variable names, pick the
        # first one based on the organization order if multiple matches.
        matching_terms_and_units.loc[matching_terms_and_units['variable_name'].isna(), 'variable_name'] = 'None'
        matching_terms_and_units = matching_terms_and_units.loc[organizations] \
            .reset_index().groupby(['variable_name']).first()

        for variable_name, row in matching_terms_and_units.iterrows():
            new_attributes = {}
            # Update variable attributes with vocabulary terms
            for item in vocabulary_attribute_list:
                if item in row.dropna():
                    new_attributes[item] = row[item]

            # Update units attribute to match vocabulary first unit listed
            if row["standardize_units"] == "unknown":
                warnings.warn(
                    "No units available within vocabularies {2} for term {0} [{1}]".format(
                        var,
                        attributes["units"],
                        matching_terms["expected_units"].to_dict(),
                        UserWarning,
                    )
                )
                new_attributes["units"] = var_units
            elif row["standardize_units"]:
                new_attributes["units"] = row["standardize_units"]
            elif row["standardize_units"] is None and "units" in attributes:
                new_attributes.pop("units")

            # Update sdn_parameter_urn term available to match trailing number with variable itself.
            if attributes.get("sdn_parameter_urn"):
                new_attributes["sdn_parameter_urn"] = re.sub(
                    r"\d\d|XX$",
                    "%02d" % parameter_code["index"],
                    new_attributes["sdn_parameter_urn"],
                )

            # If given variable name is None add to the original variable, generate attributes for a new variable.
            if variable_name == 'None':
                attributes.update(new_attributes)
            else:
                new_variable_attributes = attributes.copy()
                new_variable_attributes.update(new_attributes)
                new_variables[variable_name] = new_variable_attributes
                new_variables[variable_name]['source'] = var

    # Append new variables to generate
    metadata.update(new_variables)

    # TODO Add Warning for missing information and attributes (maybe)
    #  Example: Standard Name, P01, P06
    return metadata


def parse_odf_code_variable(odf_code: str):
    """
    Method use to parse an ODF CODE terms to a dictionary. The tool will extract the name (GF3 code),
    the index (01-99) and generate a standardized name with two digit index values if available.
    Some historical data do not follow the same standard, this tool tries to handle the issues found.

    eg
    parse_odf_code_variable("IDEN_1")={name: 'IDEN', index: 1, standardized_name: IDEN_01}
    """
    odf_code_split = odf_code.rsplit("_", 1)
    odf_code_has_index = len(odf_code_split) == 2
    gf3_code = odf_code_split[0]
    if odf_code_has_index:
        index = int(odf_code_split[1])
        return {
            "name": gf3_code,
            "index": index,
            "standardized_name": gf3_code + "_" + "{0:02g}".format(index),
        }
    # this variable has no index available
    return {"name": gf3_code, "standardized_name": gf3_code}


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
    odf_original_header.pop('variable_attributes')
    global_attributes = {
        "project": odf_header["CRUISE_HEADER"]["CRUISE_NAME"],
        "institution": odf_header["CRUISE_HEADER"]["ORGANIZATION"],
        "country_code": odf_header["CRUISE_HEADER"]["COUNTRY_INSTITUTE_CODE"],
        "cruise_number": odf_header["CRUISE_HEADER"]["CRUISE_NUMBER"],
        "cruise_name": odf_header["CRUISE_HEADER"]["CRUISE_NAME"],
        "cruise_description": odf_header["CRUISE_HEADER"]["CRUISE_DESCRIPTION"],
        "scientist": odf_header["CRUISE_HEADER"]["CHIEF_SCIENTIST"],
        "platform": odf_header["CRUISE_HEADER"]["PLATFORM"],
        "data_type": odf_header["CRUISE_HEADER"].get("DATA_TYPE", ''),
        "sampling_interval": odf_header["EVENT_HEADER"]["SAMPLING_INTERVAL"],
        "water_depth": odf_header["EVENT_HEADER"]["SOUNDING"],
        "date_created": odf_header["EVENT_HEADER"]["ORIG_CREATION_DATE"],
        "date_modified": odf_header["EVENT_HEADER"]["CREATION_DATE"],
        "history": json.dumps(
            odf_header["HISTORY_HEADER"], ensure_ascii=False, indent=False
        ),
        "comment": odf_header["EVENT_HEADER"].get("EVENT_COMMENTS", ""),
        "original_odf_header": '\n'.join(odf_header["original_header"]),
        "original_odf_header_json": json.dumps(odf_original_header, ensure_ascii=False, indent=False),
    }

    if "INSTRUMENT_HEADER" in odf_header:
        global_attributes.update({
            "instrument_type": odf_header["INSTRUMENT_HEADER"]["INST_TYPE"],
            "instrument_model": odf_header["INSTRUMENT_HEADER"]["MODEL"],
            "instrument_serial_number": odf_header["INSTRUMENT_HEADER"]["SERIAL_NUMBER"],
            "instrument_description": odf_header["INSTRUMENT_HEADER"]["DESCRIPTION"],
        })
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
        ds["start_time"].attrs[original_var_field] = "EVENT_HEADER:START_DATE_TIME"

    if convert_odf_time(odf_header["EVENT_HEADER"]["END_DATE_TIME"]):
        ds["end_time"] = convert_odf_time(odf_header["EVENT_HEADER"]["END_DATE_TIME"])
        ds["end_time"].attrs[original_var_field] = "EVENT_HEADER:END_DATE_TIME"

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
            ds["precise_time"] = ds["SYTM_01"].copy()
    else:
        ds.coords["time"] = ds["SYTM_01"]
        ds["time"].attrs[original_var_field] = "SYTM_01"

    # Make sure there's a time variable
    if "time" not in ds:
        raise RuntimeError("No time available.")

    # Coordinate variables
    # Latitude (ODF uses a place holder -99 in some of their header for latitude)
    initial_latitude = odf_header["EVENT_HEADER"].get("INITIAL_LATITUDE", -99)
    has_latitude_timeseries = "LATD_01" in ds

    if cdm_data_type in ["Profile", "TimeSeries"]:
        # Let's define latitude first, use start latitude if available
        if initial_latitude != -99:
            ds.coords["latitude"] = initial_latitude
            ds["latitude"].attrs[original_var_field] = "EVENT_HEADER:INITIAL_LATITUDE"
        # elif has_latitude_timeseries:
        #     ds.coords["latitude"] = ds["LATD_01"][0].values
        #     ds["latitude"].attrs[original_var_field] = "LATD_01[0]"

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
        # elif has_longitude_time_series:
        #     ds.coords["longitude"] = ds["LOND_01"][0].values
        #     ds["longitude"].attrs[original_var_field] = "LOND_01[0]"

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
            "units": "degrees_north",
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
            -gsw.z_from_p(ds["PRES_01"], ds["latitude"]),
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
            {"units": "m", "standard_name": "depth", "positive": "down"}
        )

    # Reorder variables
    variable_list = [var for var in ds.keys() if var not in initial_variable_order]
    variable_list.extend(list(ds.coords.keys()))
    variable_list.extend(initial_variable_order)
    ds = ds[variable_list]
    return ds
