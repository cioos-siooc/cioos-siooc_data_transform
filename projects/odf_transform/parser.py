"""
odf_parser is a module that regroup a different set of tools used to parse the ODF format which is use, maintain
and developped by the DFO offices BIO and MLI.
"""

import re

import pandas as pd
import numpy as np
import xarray as xr

from datetime import datetime

import os
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

# Commonly date place holder used within the ODF file
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


def convert_odf_time(time_string):
    """Simple tool to convert ODF timestamps to a datetime object"""
    if time_string == "17-NOV-1858 00:00:00.00":
        return pd.NaT
    elif re.search(":60.0+$", time_string):
        return pd.to_datetime(re.sub(":60.0+$", ":00.0", time_string)) + pd.Timedelta(
            "1min"
        )
    else:
        return pd.to_datetime(time_string, utc=True)


def history_input(comment, date=datetime.now()):
    return f"{date.strftime('%Y-%m-%dT%H:%M:%SZ')} {comment}\n"


def update_variable_index(varname, index):
    if varname.endswith(("XX", "01")):
        return varname[:-2] + "%02g" % index
    elif varname.endswith(("X", "1")):
        return varname[:-1] + "%01g" % index
    else:
        return varname


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

    metadata = {}  # Start with an empty dictionary
    with open(filename, encoding=encoding_format) as f:
        line = ""
        original_header = []
        # Read header one line at the time
        while "-- DATA --" not in line:
            line = f.readline()

            if "-- DATA --" in line:
                break

            # Collect each original odf header lines
            original_header.append(line)

            try:
                # Sections
                if re.match(r"\s{0,1}[A-Z_]+,{0,1}\s*\n", line):
                    section = re.search(r"([A-Z_]*)", line)[1]
                    if section not in metadata:
                        metadata[section] = [{}]
                    else:
                        metadata[section].append({})

                # Dictionary type lines (key=value)
                elif "=" in line:  # Something=This
                    key, value = [item.strip() for item in line.split("=", 1)]

                    # Drop quotes and comma
                    value = re.sub("^'|,$|',$|'$", "", value)

                    # Convert numerical values to float and integers
                    if re.match("[-+]{0,1}\d+\.\d+$", value):
                        value = float(value)
                    elif re.match("[-+]{0,1}\d+$", value):
                        value = int(value)
                    elif re.match(
                        "\d{1,2}-\w\w\w\-\d\d\d\d \d\d:\d\d:\d\d\.\d*", value
                    ):
                        try:
                            value = convert_odf_time(value)
                        except:
                            logging.warning(
                                f"Failed to read date {value} in line: {line}"
                            )

                    # Add to the metadata as a dictionary
                    # key = dict_line[0].strip().replace(" ", "_")
                    if key in metadata[section][-1]:
                        if type(metadata[section][-1][key]) is not list:
                            metadata[section][-1][key] = [metadata[section][-1][key]]
                        metadata[section][-1][key].append(value)
                    else:
                        metadata[section][-1][key] = value
                else:
                    logging.error("Unrecognizable line format: " + line)

            except:
                logging.error("Failed to read the line: " + line)

        # Simplify the single sections to a dictionary
        for section in metadata:
            if (
                len(metadata[section]) == 1
                and type(metadata[section][0]) is dict
                and section
                not in ["HISTORY_HEADER", "PARAMETER_HEADER", "QUALITY_HEADER"]
            ):
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
                "legacy_gf3_code": var_name,
                "type": att["TYPE"],
                "null_value": att["NULL_VALUE"],
                "resolution": 10 ** -att["PRINT_DECIMAL_PLACES"],
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
        related_variables = None

        # Find related variable
        if var.startswith("QQQQ"):
            # FLAG QQQQ should apply to previous variable
            related_variables = [previous_key]

            # Rename variable so that we can link by variable name
            ds = ds.rename({var: f"Q{previous_key}"})
            ds['history'] += history_input(f'Rename Parameter {var} as Q{previous_key}')
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
            # Add configuration attributes
            if var in flag_convention:
                ds[var].attrs.update(flag_convention[var])
            elif "default" in flag_convention:
                ds[var].attrs.update(flag_convention["default"])

            # Change variable type to configuration
            if "dtype" in ds[var].attrs:
                ds[var] = ds[var].astype(ds[var].attrs.pop("dtype"))

            # Match flag_values data type to variable data type
            if "flag_values" in ds[var].attrs:
                ds[var].attrs["flag_values"] = tuple(
                    np.array(ds[var].attrs["flag_values"]).astype(ds[var].dtype)
                )

        # Drop units variable from flag variables
        if "units" in ds[var].attrs:
            ds[var].attrs.pop("units")

        # Set previous key for the next iteration
        previous_key = var

    return ds


def fix_flag_variables(ds):
    """Fix different issues related to flag variables within the ODFs."""

    def _replace_flag(ds, flag_var, rename=None):
        if flag_var not in ds:
            return ds

        # Find related variables to this flag
        related_variables = [
            var
            for var in ds
            if flag_var in ds[var].attrs.get("ancillary_variables", "")
        ]

        # If no rename variable is given an it affects only one variable name it with the same name as the variable but with a preceding Q
        if rename is None:
            if len(related_variables) > 1:
                logger.error(
                    f"Multiple variables are affected by {flag_var}, I'm not sure how to rename it."
                )
            rename = "Q" + related_variables[0]

        # Rename or drop flag variable
        if rename not in ds:
            ds = ds.rename({flag_var: rename})
        elif rename in ds and (ds[flag_var].values != ds[rename].values).any():
            logger.error(
                f"{flag_var} is different than the similar {rename} flag. I'm not sure which one is the right one."
            )
        elif rename in ds and (ds[flag_var].values == ds[rename].values).all():
            ds = ds.drop(flag_var)

        # Update ancillary_variables attribute
        for var in related_variables:
            ds[var].attrs["ancillary_variables"] = (
                ds[var].attrs["ancillary_variables"].replace(flag_var, rename)
            )
        return ds

    #  List of problematic flags that need to be renamed
    temp_flag = {
        "QTE90_01": "QTEMP_01",
        "QTE90_02": "QTEMP_02",
        "QFLOR_01": None,
        "QFLOR_02": None,
        "QFLOR_03": None,
        "QCRAT_01": "QCNDC_01",
        "QCRAT_02": "QCNDC_02",
    }
    for flag, rename in temp_flag.items():
        ds = _replace_flag(ds, flag, rename)

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
        "coverage_content_type",
        "ioos_category",
        "comments",
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
        if (
            attrs == {}
            or "flag_values" in attrs
            or "legacy_gf3_code" not in attrs
            or var.startswith(("QCFF", "FFFF"))
        ):
            new_variable_order.append(var)
            continue

        # Retrieve standardize units and scale
        var_units = standardize_odf_units(attrs.get("units", "none"))
        scale = detect_reference_scale(var, attrs)
        if scale:
            # Add scale to to scale attribute
            attrs["scale"] = scale

        # Find matching vocabulary for that GF3 Code
        gf3 = GF3Code(attrs["legacy_gf3_code"])
        matching_terms = vocabulary.query(
            f"Vocabulary in{tuple(organizations)} and name=='{gf3.code}'"
        )

        # If nothing matches, move to the next one
        if matching_terms.empty:
            logger.warning(
                f"{ds.attrs['original_filename']}: "
                + f"No matching vocabulary term is available for variable {gf3.name}: {attrs}"
            )
            new_variable_order.append(var)
            continue

        # Consider only the first organization that has this term
        selected_organization = (
            matching_terms.index.get_level_values(0).drop_duplicates().tolist()
        )
        if len(selected_organization) > 1:
            present_organizations = [
                org for org in organizations if org in selected_organization
            ]
            selected_organization = present_organizations[0]
            matching_terms = matching_terms.loc[selected_organization]

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
            logger.warning(
                f"{ds.attrs['original_filename']} -> No Matching unit found for code: "
                + f"{var}: {({att: attrs[att] for att in ['long_name','units']})} in vocabulary {selected_organization}\n"
                + f"{matching_terms[['accepted_units','accepted_scale','accepted_instruments']]}"
            )
            new_variable_order.append(var)
            continue

        # Generate new variables and update original variable attributes from vocabulary
        for index, row in matching_terms_and_units.iterrows():
            # Make a copy of original variable
            if row["variable_name"]:
                # Apply suffix number of original variable
                if gf3 and gf3.code not in ("FLOR"):
                    new_variable = update_variable_index(
                        row["variable_name"], gf3.index
                    )
                else:
                    # If variable already exist within dataset and is gf3.
                    # Increment the trailing number until no similar named variable exist.
                    if row["variable_name"] in ds and gf3:
                        new_variable = None
                        trailing_number = 2
                        while new_variable == None or new_variable in ds:
                            new_variable = update_variable_index(
                                row["variable_name"], trailing_number
                            )
                            trailing_number += 1
                    else:
                        new_variable = row["variable_name"]

                # Generate new variable by either copying it or applying specified function to the initial variable
                if row["apply_function"]:
                    input_args = []
                    extra_args = re.search("lambda (.*):", row["apply_function"])
                    if extra_args:
                        for item in extra_args[1].split(","):
                            if item in var:
                                input_args.append(ds[var])
                            elif item in ds:
                                input_args.append(ds[item])
                            else:
                                input_args.append(item)

                    ds[new_variable] = xr.apply_ufunc(
                        eval(row["apply_function"]), *tuple(input_args), keep_attrs=True
                    )
                    ds.attrs["history"] += history_input(
                        f"Add Parameter: {new_variable} = {row['apply_function']}"
                    )
                else:
                    ds[new_variable] = ds[var].copy()
                    if var != new_variable:
                        ds.attrs["history"] += history_input(
                            f"Add Parameter: {new_variable} = {var}"
                        )

                new_attrs = ds[new_variable].attrs
                new_variable_order.append(new_variable)
            else:
                # Apply vocabulary to original variable
                new_attrs = ds[var].attrs
                new_variable_order.append(var)

            # Retrieve all attributes in vocabulary that have something
            new_attrs.update(row[vocabulary_attribute_list].dropna().to_dict())

            # If original data has units but vocabulary doesn't require one drop the units
            if "units" in new_attrs and row["units"] is None:
                new_attrs.pop("units")

            # Update sdn_parameter_urn and long_name terms available to match trailing number from the variable itself.
            if (
                "sdn_parameter_urn" in new_attrs
                and "legacy_gf3_code" in new_attrs
                and gf3.code not in ("FLOR")
            ):
                new_attrs["sdn_parameter_urn"] = update_variable_index(
                    new_attrs["sdn_parameter_urn"], gf3.index
                )
                # Add index to long name if bigger than 1
                if gf3.index > 1:
                    new_attrs["long_name"] += f", {gf3.index}"
    
    dropped_variables = [var for var in ds if var not in new_variable_order]
    if dropped_variables:
        ds.attrs['history'] += history_input(
            f"Drop Parameters: " + ",".join(dropped_variables)
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

