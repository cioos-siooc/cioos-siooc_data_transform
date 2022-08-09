"""
odf_parser is a module that regroup a different set of tools used to
parse the ODF format which is use, maintain and developped
by the DFO offices BIO and MLI.
"""

import logging
import re
from datetime import datetime, timezone

import gsw
import numpy as np
import pandas as pd
import xarray as xr

no_file_logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(no_file_logger, {"file": None})

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
FLAG_LONG_NAME_PREFIX = "Quality_Flag: "
ORIGINAL_PREFIX_VAR_ATTRIBUTE = "original_"


class GF3Code:
    """
    ODF GF3 Class split terms in their different components and
    standardize the convention (CODE_XX).
    """

    def __init__(self, code):
        self.code = re.search(r"^[^_]*", code)[0]
        index = re.search(r"\d+$", code)
        self.index = int(index[0]) if index else 1
        self.name = f"{self.code}_{self.index:02}" if index else self.code


def _convert_odf_time(time_string, time_zone=timezone.utc):
    """Convert ODF timestamps to a datetime object"""
    if time_string == "17-NOV-1858 00:00:00.00":
        return pd.NaT

    delta_time = (
        pd.Timedelta("1min") if re.search(r":60.0+", time_string) else pd.Timedelta(0)
    )
    if delta_time.total_seconds() > 0:
        time_string = re.sub(r":60.0+", ":00.00", time_string)
    if re.match(r"\d+-\w\w\w-\d\d\d\d\s*\d+\:\d\d\:\d\d\.\d+", time_string):
        time = datetime.strptime(time_string, r"%d-%b-%Y %H:%M:%S.%f") + delta_time
    elif re.match(r"\d\d-\w\w\w-\d\d\d\d\s*\d\d\:\d\d\:\d\d", time_string):
        time = datetime.strptime(time_string, r"%d-%b-%Y %H:%M:%S") + delta_time
    else:
        logger.warning("Unknown time format: %s", time_string)
        time = pd.to_datetime(time_string).to_pydatetime() + delta_time
    return time.replace(tzinfo=time_zone)


def history_input(comment, date=datetime.now(timezone.utc)):
    """Genereate a CF standard history line: Timstamp comment"""
    return f"{date.strftime('%Y-%m-%dT%H:%M:%SZ')} {comment}\n"


def update_variable_index(varname, index):
    """Standardize variables trailing number to two digits"""
    if varname.endswith(("XX", "01")):
        return f"{varname[:-2]}{index:02}"
    elif varname.endswith(("X", "1")):
        return f"{varname[:-1]}{index:01}"
    else:
        return varname


def read(filename, encoding_format="Windows-1252"):
    """
    Read_odf
    Read_odf parse the odf format used by some DFO organisation to python list of
    dictionary format and pandas dataframe. Once converted, the output can easily
    be converted to netcdf format.

    Steps applied:
        1. Read line by line an ODF header and distribute each lines in a list of
           list and dictionaries.
            a. Lines associated with a character at the beginning are considered a section.
            b. Lines starting white spaces are considered items in preceding section.
            c. Repeated sections are grouped as a list
            d. Each section items are grouped as a dictionary
            e. dictionary items are converted to datetime (deactivated), string, integer or
                float format.
        2. Read the data  following the header with Pandas.read_csv() method
            a. Use defined separator  to distinguish columns (default multiple white spaces).
            b. Convert each column of the pandas data frame to the matching format specified in
            the TYPE attribute of the ODF associated PARAMETER_HEADER

    read_odf is a simple tool that  parse the header metadata and data from an DFO
    ODF file to a list of dictionaries.
    :param filename: ODF file to read
    :param encoding_format: odf encoding format
     start of the data.
    :return:
    """

    def _cast_value(value: str):
        """Attemp to cast value in line "key=value" of ODF header:
        - integer
        - float
        - date
        - else string
        """
        # Drop quotes and comma
        value = re.sub(r"^'|,$|',$|'$", "", value)

        # Convert numerical values to float and integers
        if re.match(r"[-+]{0,1}\d+\.\d+$", value):
            return float(value)
        elif re.match(r"[-+]{0,1}\d*\.\d+[ED][+-]\d+$", value):
            return float(value.replace("D","E"))
        elif re.match(r"[-+]{0,1}\d+$", value):
            return int(value)
        elif re.match(r"^\d{1,2}-\w\w\w\-\d\d\d\d\s*\d\d:\d\d:\d\d\.*\d*$", value):
            try:
                return _convert_odf_time(value)
            except pd.errors.ParserError:
                logger.warning(
                    "Failed to read date '%s' in line: %s",
                    value,
                    line,
                    exc_info=True,
                )
                return value
        # Empty lines
        elif re.match(r"^\s*$", value):
            return None
        # if do not match any conditions return unchanged
        return value

    metadata = {}  # Start with an empty dictionary
    with open(filename, encoding=encoding_format) as f:
        line = ""
        original_header = []
        # Read header one line at the time
        for line in f:
            line = line.replace("\n", "")
            original_header.append(line)
            # Read header only
            if "-- DATA --" in line:
                break

            # Sections
            if re.match(r"\s{0,1}[A-Z_]+,{0,1}\s*", line):
                section = re.search(r"\s*([A-Z_]*)", line)[1]
                if section not in metadata:
                    metadata[section] = [{}]
                else:
                    metadata[section].append({})
                continue

            elif "=" in line:  # Something=This
                key, value = [item.strip() for item in line.split("=", 1)]
            else:
                logger.error("Unrecognizable line format: %s", line)
                continue

            # Parse metadata row
            key = key.strip().replace(" ", "_")
            value = _cast_value(value)

            # Add to the metadata as a dictionary
            if key in metadata[section][-1]:
                if not isinstance(metadata[section][-1][key], list):
                    metadata[section][-1][key] = [metadata[section][-1][key]]
                metadata[section][-1][key].append(value)
            else:
                metadata[section][-1][key] = value

        # Simplify the single sections to a dictionary
        temp_metadata = metadata.copy()
        for section, items in metadata.items():
            if (
                len(items) == 1
                and isinstance(items[0], dict)
                and section
                not in ["HISTORY_HEADER", "PARAMETER_HEADER", "QUALITY_HEADER"]
            ):
                temp_metadata[section] = temp_metadata[section][0]
        metadata = temp_metadata

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
            elif "WMO_CODE" in att:
                var_name = GF3Code(att["WMO_CODE"]).name
            else:
                raise RuntimeError("Unrecognizable ODF variable attributes")

            attributes = {
                "long_name": att.get("NAME"),
                "units": att.get("UNITS"),
                "legacy_gf3_code": var_name,
                "null_value": att["NULL_VALUE"],
                "resolution": 10 ** -att["PRINT_DECIMAL_PLACES"],
            }

            if attributes["units"]:
                # Standardize units
                attributes["units"] = attributes["units"].replace("**", "^")

            # Add those variable attributes to the metadata output
            metadata["variable_attributes"].update({var_name: attributes})
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
            na_values={
                key: att.pop("null_value")
                for key, att in metadata["variable_attributes"].items()
            },
            date_parser=_convert_odf_time,
            parse_dates=time_columns,
            encoding=encoding_format,
        )

    # Review N variables
    if len(data_raw.columns) != len(metadata["PARAMETER_HEADER"]):
        raise RuntimeError(
            f"{len(data_raw.columns)}/{len(metadata['PARAMETER_HEADER'])} variables were detected"
        )

    # Make sure that timezone is UTC, GMT or None
    # (This may not be necessary since we're looking at the units later now)
    if time_columns:
        for parm in time_columns:
            units = metadata["variable_attributes"][parm].get(
                ORIGINAL_PREFIX_VAR_ATTRIBUTE + "UNITS"
            )
            if units not in [None, "none", "(none)", "GMT", "UTC", "seconds"]:
                logger.warning(
                    "%s: %s has UNITS (timezone) of %s", filename, parm, units
                )

    return metadata, data_raw


def odf_flag_variables(dataset, flag_convention=None):
    """
    odf_flag_variables handle the different conventions used within the ODF files
    over the years and map them to the CF standards.
    """

    # Loop through each variables and detect flag variables
    previous_key = None
    for var in dataset:
        related_variables = None

        # Find related variable
        if var.startswith("QQQQ"):
            # FLAG QQQQ should apply to previous variable
            related_variables = [previous_key]

            # Rename variable so that we can link by variable name
            dataset = dataset.rename({var: f"Q{previous_key}"})
            dataset.attrs["history"] += history_input(
                f"Rename Parameter {var} as Q{previous_key}"
            )
            var = f"Q{previous_key}"

        elif var.startswith(("QCFF", "FFFF")):
            # This is a general flag
            related_variables = [var for var in dataset if not var.startswith("Q")]

        elif var.startswith("Q") and var[1:] in dataset:
            # Q  Format is usually Q+[PCODE] of the associated variable
            related_variables = [var[1:]]

        else:
            # If the variable isn't a flag variable, go to the next iteration
            # Set previous key for the next iteration
            previous_key = var
            continue

        # Add flag variable to related variable ancillary_variables attribute
        for related_variable in related_variables:
            if "ancillary_variables" in dataset[related_variable].attrs:
                dataset[related_variable].attrs["ancillary_variables"] += f" {var}"
            else:
                dataset[related_variable].attrs["ancillary_variables"] = var

        # Add flag convention attributes if available within config file
        if flag_convention:
            # Add configuration attributes
            if var in flag_convention:
                dataset[var].attrs.update(flag_convention[var])
            elif "default" in flag_convention:
                dataset[var].attrs.update(flag_convention["default"])

            # Change variable type to configuration
            if "dtype" in dataset[var].attrs:
                dataset[var] = dataset[var].astype(dataset[var].attrs.pop("dtype"))

            # Match flag_values data type to variable data type
            if "flag_values" in dataset[var].attrs:
                dataset[var].attrs["flag_values"] = tuple(
                    np.array(dataset[var].attrs["flag_values"]).astype(
                        dataset[var].dtype
                    )
                )

        # Drop units variable from flag variables
        if "units" in dataset[var].attrs:
            dataset[var].attrs.pop("units")

        # Set previous key for the next iteration
        previous_key = var

    return dataset


def fix_flag_variables(dataset):
    """Fix different issues related to flag variables within the ODFs."""

    def _replace_flag(dataset, flag_var, rename=None):
        if flag_var not in dataset:
            return dataset

        # Find related variables to this flag
        related_variables = [
            var
            for var in dataset
            if flag_var in dataset[var].attrs.get("ancillary_variables", "")
        ]

        # Update long_name if flag is related to only one variable
        if len(related_variables) == 1:
            dataset[flag_var].attrs["long_name"] = (
                FLAG_LONG_NAME_PREFIX + dataset[related_variables[0]].attrs["long_name"]
            )

        # If no rename and affects only one variable. Name it Q{related_variable}
        if rename is None:
            if len(related_variables) > 1:
                logger.error(
                    "Multiple variables are affected by %s, I'm not sure how to rename it.",
                    flag_var,
                )
            rename = "Q" + related_variables[0]

        # Rename or drop flag variable
        if rename not in dataset:
            dataset = dataset.rename({flag_var: rename})
        elif (
            rename in dataset
            and (dataset[flag_var].values != dataset[rename].values).any()
        ):
            logger.error(
                "%s is different than %s flag. I'm not sure which one is the right one.",
                flag_var,
                rename,
            )
        elif (
            rename in dataset
            and (dataset[flag_var].values == dataset[rename].values).all()
        ):
            dataset = dataset.drop(flag_var)

        # Update ancillary_variables attribute
        for var in related_variables:
            dataset[var].attrs["ancillary_variables"] = (
                dataset[var].attrs["ancillary_variables"].replace(flag_var, rename)
            )
        return dataset

    #  List of problematic flags that need to be renamed
    temp_flag = {
        "QTE90_01": "QTEMP_01",
        "QTE90_02": "QTEMP_02",
        "QFLOR_01": None,
        "QFLOR_02": None,
        "QFLOR_03": None,
        "QCRAT_01": "QCNDC_01",
        "QCRAT_02": "QCNDC_02",
        "QTURB_01": None,
        "QWETECOBB_01": None,
        "QUNKN_01": None,
        "QUNKN_02": None,
    }
    for flag, rename in temp_flag.items():
        dataset = _replace_flag(dataset, flag, rename)

    return dataset


def get_vocabulary_attributes(ds, organizations=None, vocabulary=None):
    """
    This method is use to retrieve from an ODF variable code, units and units,
    matching vocabulary terms available.
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

            for _, value in attributes.items():
                if isinstance(value, str) and re.search(
                    scale_search, value, re.IGNORECASE
                ):
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
        if accepted_terms is None:
            # No required term
            return True

        accepted_units_list = accepted_terms.split("|")
        if any(unit in ["none", "dimensionless"] for unit in accepted_units_list):
            # Include unitless data
            return True
        elif term in accepted_units_list:
            # Match exactely one of the listed terms
            return True
        elif regexp and re.search(accepted_terms, term, search_flag):
            # Match expected term
            return True
        else:
            # term do not match expected terms
            return False

    def _match_term(units, scale, variable_instrument, global_instrument):
        # Among these matching terms find matching ones
        match_units = matching_terms["accepted_units"].apply(
            lambda x: _review_term(units, x)
        )
        match_scale = matching_terms["accepted_scale"].apply(
            lambda x: _review_term(scale, x)
        )
        match_instrument = matching_terms["accepted_instruments"].apply(
            lambda x: _review_term(
                variable_instrument, x, regexp=True, search_flag=re.IGNORECASE
            )
        )
        match_instrument_global = matching_terms["accepted_instruments"].apply(
            lambda x: _review_term(
                global_instrument, x, regexp=True, search_flag=re.IGNORECASE
            )
        )
        return match_units & match_scale & (match_instrument | match_instrument_global)

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
    new_variable_order = []
    for var in ds:
        # Ignore variables with no attributes and flag variables
        if (
            ds[var].attrs == {}
            or "flag_values" in ds[var].attrs
            or "legacy_gf3_code" not in ds[var].attrs
            or var.startswith(("QCFF", "FFFF"))
        ):
            new_variable_order.append(var)
            continue

        # Retrieve standardize units and scale
        scale = detect_reference_scale(var, ds[var].attrs)
        if scale:
            # Add scale to to scale attribute
            ds[var].attrs["scale"] = scale

        # Find matching vocabulary for that GF3 Code
        gf3 = GF3Code(ds[var].attrs["legacy_gf3_code"])
        matching_terms = vocabulary.query(
            f"Vocabulary in {tuple(organizations)} and name == @gf3.code"
        )

        # If nothing matches, move to the next one
        if matching_terms.empty:
            logger.warning(
                "No matching vocabulary term is available for variable %s: %s",
                gf3.name,
                ds[var].attrs,
            )
            new_variable_order.append(var)
            continue

        # Consider only the first organization that has this term
        selected_organization = matching_terms.index.values[0][0]
        matching_terms = matching_terms.loc[matching_terms.index.values[0][0]]

        # Among these matching terms find matching ones
        match_result = _match_term(
            ds[var].attrs.get("units", "none"),
            ds[var].attrs.get("scale"),
            ds[var].attrs["long_name"],
            (
                f"{ds.attrs.get('instrument_type','')} {ds.attrs.get('instrument_model',)}"
            ).strip(),
        )

        # Select only the terms that matches all the units/scale/instrument conditions
        matching_terms_and_units = matching_terms.loc[match_result]

        # No matching term, give a warning if not a flag and move on to the next iteration
        if len(matching_terms_and_units) == 0:
            logger.warning(
                "No Matching unit found for code: %s: %s in vocabulary %s",
                var,
                ({att: ds[var].attrs[att] for att in ["long_name", "units"]}),
                selected_organization,
            )
            new_variable_order.append(var)
            continue

        # Generate new variables and update original variable attributes from vocabulary
        for _, row in matching_terms_and_units.iterrows():
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
                        while new_variable is None or new_variable in ds:
                            new_variable = update_variable_index(
                                row["variable_name"], trailing_number
                            )
                            trailing_number += 1
                    else:
                        new_variable = row["variable_name"]

                # Generate new variable
                if row["apply_function"]:
                    input_args = []
                    extra_args = re.search(r"lambda (.*):", row["apply_function"])
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

            # Update sdn_parameter_urn and long_name terms available
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
        ds.attrs["history"] += history_input(
            f"Drop Parameters: {','.join(dropped_variables)}"
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
