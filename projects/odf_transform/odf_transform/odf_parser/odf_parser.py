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
                metadata[section][-1][dict_line[0]] = dict_line[1]

            else:
                assert RuntimeError, "Can't understand the line: " + line

        # Simplify the single sections to a dictionary
        for section in metadata:
            if len(metadata[section]) == 1 and type(metadata[section][0]) is dict:
                metadata[section] = metadata[section][0]

        # READ PARAMETER_HEADER
        # Define first the variable names and the type.
        variable_attributes = {}
        # Variable names and related attributes
        for att in metadata["PARAMETER_HEADER"]:
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

            # Make sure that the variable name is GF3 term (opt: two digit number)
            variable_attributes[var_name["standardized_name"]] = att

        # Retrieve list of time variables
        time_columns = [
            key
            for key, att in variable_attributes.items()
            if key.startswith("SYTM") or att["TYPE"] == "SYTM"
        ]
        if not time_columns:
            time_columns = False

        # Read Data with Pandas
        data_raw = pd.read_csv(
            f,
            delimiter=r"\s+",
            quotechar="'",
            header=None,
            names=variable_attributes.keys(),
            dtype={
                key: odf_dtypes[att.get("TYPE")]
                for key, att in variable_attributes.items()
            },
            na_values={
                key: att.get("NULL_VALUE") for key, att in variable_attributes.items()
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

    # Add a original_ before each variable attributes from the ODF
    metadata["variable_attributes"] = {
        var: {original_prefix_var_attribute + key: value for key, value in att.items()}
        for var, att in variable_attributes.items()
    }
    # Make sure that timezone is UTC, GMT or None
    if time_columns:
        for parm in time_columns:
            units = metadata["variable_attributes"][parm].get(
                original_prefix_var_attribute + "UNITS"
            )
            if units not in [None, "none", "(none)", "GMT", "UTC", "seconds"]:
                warnings.warn(
                    "{0} has UNITS(timezone) of {1}".format(parm, units), UserWarning
                )

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

        # Set previous key for the next iteration
        previous_key = var

        # If the variable isn't a flag variable, go to the next iteration
        if not is_flag_column:
            continue

        # If flag is specific to a variable, try to confirm if the odf name of the related variable match the
        # flag name. If yes, add this flag variable to the ancillary_attribute of the related variable
        if related_variable:
            # FIRST MAKE SURE THE RELATED VARIABLE IS THE RIGHT ONE
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
                att["original_NAME"],
                1,
                re.IGNORECASE,
            )
            # Flag name do not match either variable name or code, give a warning.
            if related_variable_name not in metadata[related_variable].get(
                "original_NAME"
            ) and related_variable_name not in metadata[related_variable].get(
                "original_CODE"
            ):
                warnings.warn(
                    "{0}[{1}] flag was matched to the variable {2} but ODF attributes {3} do not match".format(
                        var,
                        att["original_NAME"],
                        related_variable,
                        {
                            key: metadata[related_variable].get(key)
                            for key in ["original_NAME", "original_CODE"]
                        },
                    ),
                    UserWarning,
                )

            # Standardize long name attribute of flag variables
            if "name" in metadata[related_variable]:
                att["long_name"] = (
                    flag_long_name_prefix + metadata[related_variable]["name"]
                )
            else:
                att["long_name"] = flag_long_name_prefix + related_variable

            # Add ancillary_variables attribute
            if "ancillary_variables" in metadata[related_variable]:
                metadata[related_variable]["ancillary_variables"] += ",{0}".format(var)
            else:
                metadata[related_variable]["ancillary_variables"] = var

        # Add flag convention attributes if available within config file
        if flag_convention:
            if var in flag_convention:
                att.update(flag_convention[var])
            elif "default" in flag_convention:
                att.update(flag_convention["default"])

        # TODO rename QQQQ_XX flag variables to Q[related_variables] so that ERDDAP can easily amalgamate them!

    return metadata


def get_vocabulary_attributes(
    metadata, organizations=None, vocabulary=None, vocabulary_attribute_list=None
):
    """
    This method is use to retrieve from an ODF file each variable code and corresponding related
    vocabularies associated to the organization and variable name.
    Flag columns are also reviewed and matched to the appropriate variable.
    """
    def _compare_units(unit, vocab_units):
        """Simple tool to compare "|" separated units in the Vocabulary expected unit list.
        Return first unit if any is matching.
        - None if empty or expected to be empty
        - False if not matching untis
        - standard unit string if matching any of the terms listed.
        """
        if vocab_units:
            # Split unit list and convert None or dimensionless to None
            unit_list = []
            for item in vocab_units.split("|"):
                if item.lower() in ['none', 'dimensionless']:
                    unit_list.append(None)
                else:
                    unit_list.append(item)

            standard_unit = unit_list[0]
        else:
            unit_list = []
            standard_unit = None

        if unit in unit_list:
            return standard_unit
        elif unit is None and standard_unit is None:
            return None
        elif unit and standard_unit is None:
            return 'unknown'
        elif unit not in unit_list:
            return False

    # Define vocabulary default list of variables to import as attributes
    if vocabulary_attribute_list is None:
        vocabulary_attribute_list = (
            "name",
            "standard_name",
            "sdn_parameter_urn",
            "sdn_parameter_name",
        )

    # Find matching vocabulary
    for var, attributes in metadata.items():
        # Separate the parameter_code from the number at the end of the variable
        parameter_code = parse_odf_code_variable(var)

        # We already standardized all the flags with the standard long_name attribute. Let's detect them with that.
        flag_column = attributes.get("long_name", "").startswith(
            flag_long_name_prefix
        ) or parameter_code["name"] in ["QCFF", "FFFF"]

        # Get ODF variable units and make some simple standardization
        var_units = attributes.get("original_UNITS", "none")
        var_units = standardize_odf_units(var_units)

        # Find matching vocabularies and code and sort by given vocabularies
        matching_terms = vocabulary[
            vocabulary.index.isin(organizations, level=0)
            & vocabulary.index.isin([parameter_code["name"]], level=1)
        ].copy()

        # Among these matching terms compare units
        matching_terms['standardize_units'] = matching_terms['expected_units'].apply(
            lambda x: _compare_units(var_units, x)
        )

        # Drop the terms with no matching units
        matching_terms_and_units = matching_terms[~matching_terms['standardize_units'].isin([False])]

        # No matching term, give a warning if not a flag and move on to the next iteration
        if len(matching_terms_and_units) == 0:
            if flag_column:
                continue
            elif len(matching_terms) == 0:
                warnings.warn(
                    "{0}[{1}] not available in vocabularies: {2}".format(
                        parameter_code["name"], attributes["original_UNITS"], organizations
                    ),
                    UserWarning,
                )
            elif len(matching_terms_and_units) == 0:
                # If it has found a matching terms but not unit.
                warnings.warn(
                    "No Matching unit found for {0} [{1}] in: {2}".format(
                        var,
                        attributes.get("original_UNITS"),
                        matching_terms["expected_units"].to_dict(),
                    ),
                    UserWarning,
                )
            continue

        # Sort matching terms by vocabulary order given and pick the very first one
        matched_terms = matching_terms_and_units.reindex(organizations, level=0).iloc[0]

        # Update variable attributes with vocabulary terms
        for item in vocabulary_attribute_list:
            if item in matched_terms.dropna():
                attributes[item] = matched_terms[item]

        # Update units attribute to match vocabulary
        if matched_terms['standardize_units'] == 'unknown':
            warnings.warn(
                "No units available within vocabularies {2} for term {0} [{1}]".format(
                    var,
                    attributes["original_UNITS"],
                    matching_terms["expected_units"].to_dict(),
                    UserWarning,
                )
            )
            attributes['units'] = var_units
        elif matched_terms['standardize_units']:
            attributes['units'] = matched_terms['standardize_units']
        elif matching_terms['standardize_units'] is None and 'units' in attributes:
            attributes.pop('units')

        # Update sdn_parameter_urn term available to match trailing number with variable itself.
        if attributes.get("sdn_parameter_urn"):
            attributes["sdn_parameter_urn"] = re.sub(
                r"\d\d$",
                "%02d" % parameter_code["index"],
                attributes["sdn_parameter_urn"],
            )

    # TODO Add Warning for missing information and attributes (maybe)
    #  Example: Standard Name, P01, P02
    return metadata


def parse_odf_code_variable(var_name):
    """
    Method use to parse an ODF CODE terms to a dictionary. The tool will extract the name (GF3 code),
    the index (01-99) and generate a standardized name with two digit index values if available.
    Some historical data do not follow the same standard, this tool tries to handle the issues found.
    """
    var_list = var_name.rsplit("_", 1)
    var_dict = {"name": var_list[0]}
    var_dict["standardized_name"] = var_dict["name"]
    if len(var_list) > 1 and var_list[1] not in [""]:
        var_dict["index"] = int(var_list[1])
        var_dict["standardized_name"] += "_{0:02.0f}".format(var_dict["index"])
    elif len(var_list) > 1 and var_list[1] == "":
        var_dict["standardized_name"] += "_"

    return var_dict


def standardize_odf_units(unit_string):
    """
    Units strings were manually written within the ODF files.
    We're trying to standardize all the different issues found.
    """

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
    global_attributes = {
        "project": odf_header["CRUISE_HEADER"]["CRUISE_NAME"],
        "institution": odf_header["CRUISE_HEADER"]["ORGANIZATION"],
        "history": json.dumps(
            odf_header["HISTORY_HEADER"], ensure_ascii=False, indent=False
        ),
        "comment": odf_header["EVENT_HEADER"].get("EVENT_COMMENTS", ""),
        "header": json.dumps(odf_header, ensure_ascii=False, indent=False),
    }
    return global_attributes


def convert_odf_time(time_string):
    """Simple tool to convert ODF timestamps to a datetime object"""
    if time_string == "17-NOV-1858 00:00:00.00":
        return None
    else:
        return pd.to_datetime(time_string, utc=True)


def generate_variables_from_header(
    ds, odf_header, cdm_data_type, original_var_field="original_variable"
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
            ds["time"] = ds["start_time"].copy()
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
