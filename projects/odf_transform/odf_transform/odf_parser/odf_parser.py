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
odf_dtypes = {'DOUB': 'float64', 'SING': 'float32', 'DOUBLE': 'float64',
              'SYTM': str, 'INTE': 'int32', 'CHAR': str, 'QQQQ': 'int32'}

# Commonly date place holder used within the ODF files
odf_time_null_value = (dt.datetime.strptime("17-NOV-1858 00:00:00.00", '%d-%b-%Y %H:%M:%S.%f') -
                       dt.datetime(1970, 1, 1)).total_seconds()

flag_long_name_prefix = 'QualityFlag: '
original_prefix_var_attribute = 'original_'


def read(filename,
         encoding_format='Windows-1252'
         ):
    """
    Read_odf
    Read_odf parse the odf format used by some DFO organisation to python list of diectionary format and
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
    header_end = '-- DATA --'
    data_delimiter = r'\s+'
    quotechar = '\''
    parameter_section = 'PARAMETER_HEADER'
    variable_type = 'TYPE'
    null_value = 'NULL_VALUE'
    section_items_minimum_whitespaces = 2

    def _convert_to_number(value):
        """ Simple method to try to convert input (string, literals) to float or integer."""
        try:
            floated = float(value)
            if floated.is_integer():
                return int(floated)
            return floated
        except ValueError:
            return value

    metadata = {}  # Start with an empty dictionary
    with open(filename, encoding=encoding_format) as f:
        line = ''
        original_header = []
        # Read header one line at the time
        while header_end not in line:
            line = f.readline()
            # Drop some characters that aren't useful
            line = re.sub(r'\n|,$', '', line)

            # Collect each original odf header lines
            original_header.append(line)

            # Detect the end of the header
            if header_end in line:
                # May also be stop by the while condition
                break

            # Sections
            if re.match(r'^\s{0,' + str(section_items_minimum_whitespaces - 1) + r'}\w', line):
                section = line.replace('\n', '').replace(',', '')
                section = re.sub(r'^\s*|\s$', '', section)  # Ignore white spaces before and after
                if section not in metadata:
                    metadata[section] = [{}]
                else:
                    metadata[section].append({})

            # Dictionary type lines (key=value)
            elif re.match(r'^\s{' + str(section_items_minimum_whitespaces) + r'}\s*\w', line):  # Something=This
                dict_line = re.split(r'=', line, maxsplit=1)  # Make sure that only the first = is use to split
                dict_line = [re.sub(r'^\s+|\s+$', '', item) for item in dict_line]  # Remove trailing white spaces

                if re.match(r'\'.*\'', dict_line[1]):  # Is delimited by double quotes, definitely a string
                    # Drop the quote signs and the white spaces before and after
                    dict_line[1] = str(re.sub(r'^\s*|\s*$', '', dict_line[1][1:-1]))
                else:
                    # Try to convert the value of the dictionary in an integer or float
                    dict_line[1] = _convert_to_number(dict_line[1])

                # Add to the metadata as a dictionary
                metadata[section][-1][dict_line[0]] = dict_line[1]

            elif re.match(r'^\s+.+', line):  # Unknown line format (likely comments) doesn't seem to have any examples
                # TODO this hasn't been tested yet I haven't try an example with not a dictionary like line
                metadata[section].append(line)
            else:
                assert RuntimeError, "Can't understand the line: " + line

        # Simplify the single sections to a dictionary
        for section in metadata:
            if len(metadata[section]) == 1 and \
                    type(metadata[section][0]) is dict:
                metadata[section] = metadata[section][0]

        # READ ODF DATA SECTION
        # Define first the variable names and the type.
        variable_attributes = {}
        # Variable names and related attributes
        for att in metadata[parameter_section]:
            if 'CODE' in att:
                var_name = parse_odf_code_variable(att['CODE'])
            elif 'NAME' in att and 'WMO_CODE' in att and att['NAME'].startswith(att['WMO_CODE']):
                var_name = parse_odf_code_variable(att['NAME'])
            else:
                raise RuntimeError('Unrecognizable ODF variable attributes')

            # Make sure that the variable name is GF3 term (opt: two digit number)
            variable_attributes[var_name['standardized_name']] = att
            # Retrieve list of time variables
        time_columns = [key for key, att in variable_attributes.items()
                        if key.startswith('SYTM') or att['TYPE'] == 'SYTM']
        if not time_columns:
            time_columns = False

        # Read with Pandas
        data_raw = pd.read_csv(f, delimiter=data_delimiter, quotechar=quotechar, header=None,
                               names=variable_attributes.keys(),
                               dtype={key: odf_dtypes[att.get(variable_type)]
                                      for key, att in variable_attributes.items()},
                               na_values={key: att.get(null_value) for key, att in variable_attributes.items()},
                               parse_dates=time_columns,
                               encoding=encoding_format, )

    # Make sure that there's the same amount of variables read versus what is suggested in the header
    if len(data_raw.columns) != len(metadata[parameter_section]):
        raise RuntimeError('{0} variables were detected in the data versus {1} in the header.'
                           .format(len(data_raw.columns), len(metadata[parameter_section])))

        # Add a original_ before each variable attributes from the ODF
    metadata['variable_attributes'] = {var: {
        original_prefix_var_attribute + key: value
        for key, value in att.items()
    }
        for var, att in variable_attributes.items()
    }
    # # Make sure that timezone is UTC, GMT or None
    if time_columns:
        for parm in time_columns:
            units = metadata['variable_attributes'][parm].get(original_prefix_var_attribute + 'UNITS')
            if units not in [None, 'none', '(none)', 'GMT', 'UTC', 'seconds']:
                warnings.warn('{0} has UNITS(timezone) of {1}'.format(parm, units), UserWarning)

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
        is_q_flag = var.startswith("Q") and var[:1] in metadata.keys()
        is_qqqq_flag = odf_var_name['name'] == 'QQQQ'
        is_general_flag = odf_var_name['name'] in ['QCFF', 'FFFF']
        is_flag_column = is_q_flag or is_qqqq_flag or is_general_flag

        # Find related variable
        if is_qqqq_flag:
            # MLI FLAG QQQQ should apply to previous variable
            related_variable = previous_key
        elif is_q_flag:
            # Q  Format is usually Q+[PCODE] of the associated variable
            related_variable = var[1:]
            # Make sure that related_variable do exist!
            if related_variable not in metadata:
                warnings.warn('{0} flag is referring to {1} which is not available as variable'
                              .format(var, related_variable), UserWarning)

        # Set previous key for the next run
        previous_key = var

        # Make sure that the flag column relates to a specific variable and try to make sure it's the right match.
        # Try to confirm by matching either name or code
        if is_flag_column:
            if not is_general_flag and related_variable:
                # Drop odf flag name prefix
                flag_name_with_no_prefix = re.sub(r"quality\sflag.*:\s*|quality flag of ", '',
                                                  att['original_NAME'], 1, re.IGNORECASE)
                # Flag name do not match either variable name or code, give a warning.
                if related_variable and \
                        flag_name_with_no_prefix not in metadata[related_variable].get('original_NAME') and \
                        flag_name_with_no_prefix not in metadata[related_variable].get('original_CODE'):
                    warnings.warn(
                        '{0}[{4}] flag was matched to referring to {1} but odf variable name[{2}] or code[{3}] do not match'
                            .format(var,
                                    related_variable,
                                    metadata[related_variable].get('original_NAME'),
                                    metadata[related_variable].get('original_CODE'),
                                    att['original_NAME']),
                        UserWarning)

            # Standardize Flag variable attributes, related variable and add convention attributes
            if related_variable:
                # Add long name attribute which is generally QUALITY_FLAG: [variable it affects]
                if 'name' in metadata[related_variable]:
                    att['long_name'] = flag_long_name_prefix + metadata[related_variable]['name']
                else:
                    att['long_name'] = flag_long_name_prefix + related_variable

                # Add ancillary_variables attribute
                if 'ancillary_variables' not in metadata[related_variable]:
                    metadata[related_variable]['ancillary_variables'] = var
                elif 'ancillary_variables' in metadata[related_variable]:
                    metadata[related_variable]['ancillary_variables'] += ',{0}'.format(var)
                else:
                    warnings.warn('unknown ancillary flag format attribute', UserWarning)

            # Add flag convention attributes if available within config file
            if flag_convention:
                if var in flag_convention:
                    att.update(flag_convention[var])
                elif 'default' in flag_convention:
                    att.update(flag_convention['default'])

        # TODO rename QQQQ_XX flag variables to Q[related_variables] so that ERDDAP can easily amalgamate them!

    return metadata


def get_vocabulary_attributes(metadata,
                              organizations=None,
                              vocabulary=None,
                              vocabulary_attribute_list=None
                              ):
    """
    This method is use to retrieve from an ODF file each variable code and corresponding related
    vocabularies associated to the organization and variable name.
    Flag columns are also reviewed and matched to the appropriate variable.
    """
    # Define vocabulary default list of variables to import as attributes
    if vocabulary_attribute_list is None:
        vocabulary_attribute_list = ('name', 'standard_name',
                                     'sdn_parameter_urn', 'sdn_parameter_name')

    # Find matching vocabulary
    for var, att in metadata.items():
        # Separate the parameter_code from the number at the end of the variable
        parameter_code = parse_odf_code_variable(var)
        att['parameter_code'] = parameter_code['name']
        att['parameter_code_number'] = parameter_code['index']

        flag_column = att.get('long_name', '').startswith(flag_long_name_prefix) or \
                      parameter_code['name'] in ['QCFF', 'FFFF']

        # Loop through each organisations and find the matching parameter_code within the vocabulary
        if vocabulary is not None and var not in ['SYTM_01']:
            # Find matching vocabularies and code and sort by given vocabularies
            matching_terms = vocabulary[vocabulary.index.isin(organizations, level=0) &
                                        vocabulary.index.isin([parameter_code['name']], level=1)]

            # Iterate over each matching vocabulary and review units
            if len(matching_terms) > 0:
                # Sort by given vocabulary order
                matching_terms = matching_terms.reindex(organizations, level=0)

                var_units = att.get('original_UNITS', '')
                if var_units:
                    var_units = standardize_odf_units(var_units)

                for index, row in matching_terms.iterrows():
                    # Compare actual units to what's expected in the vocabulary
                    if row.isna()['expected_units'] or \
                            var_units in row['expected_units'].split('|') or \
                            re.search('none|dimensionless', row['expected_units'], re.IGNORECASE) is not None:

                        # Add attributes from vocabulary
                        att.update(row.filter(vocabulary_attribute_list).dropna().to_dict())

                        # Standardize units
                        if not flag_column:
                            # Standardized units by selecting the very first possibility
                            # or not giving unit attribute if none
                            if type(row['expected_units']) is str:
                                att['units'] = row['expected_units'].split('|')[0]
                            elif var_units not in ['none']:
                                att['units'] = var_units

                            # No units available make sure it's the same in the data
                            if row.isna()['expected_units'] and \
                                    var_units not in [None, 'none']:
                                warnings.warn('No units available within vocabularies {2} for term {0} [{1}]'
                                              .format(var, att['original_UNITS'],
                                                      matching_terms['expected_units'].to_dict(), UserWarning))
                        break

                    # Will make it here if don't find any matching untis
                    warnings.warn('No Matching unit found for {0} [{1}] in: {2}'
                                  .format(var, att.get('original_UNITS'),
                                          matching_terms['expected_units'].to_dict()), UserWarning)
            elif not flag_column:
                # If no matching vocabulary exist let it know
                warnings.warn('{0}[{1}] not available in vocabularies: {2}'.format(parameter_code['name'],
                                                                                   att['original_UNITS'],
                                                                                   organizations),
                              UserWarning)

            # Update sdn_parameter_urn term available to match trailing number with variable itself.
            if 'sdn_parameter_urn' in att and \
                    type(att['sdn_parameter_urn']) is str:
                att['sdn_parameter_urn'] = re.sub(r'\d\d$',
                                                  '%02d' % att['parameter_code_number'],
                                                  att['sdn_parameter_urn'])
            if 'name' in att and \
                    type(att['name']) is str:
                att['name'] = re.sub(r'\d\d$',
                                     '%02d' % att['parameter_code_number'],
                                     att['name'])

    # TODO Add Warning for missing information and attributes (maybe)
    #  Example: Standard Name, P01, P02
    return metadata


def parse_odf_code_variable(var_name):
    """
    Method use to parse an ODF CODE terms to a dictionary. The tool will extract the name (GF3 code),
    the index (01-99) and generate a standardized name with two digit index values if available.
    Some historical data do not follow the same standard, this tool tries to handle the issues found.
    """
    var_list = var_name.rsplit('_', 1)
    var_dict = {'name': var_list[0]}
    var_dict['standardized_name'] = var_dict['name']
    if len(var_list) > 1 and var_list[1] not in ['']:
        var_dict['index'] = int(var_list[1])
        var_dict['standardized_name'] += '_{0:02.0f}'.format(var_dict['index'])
    elif len(var_list) > 1 and var_list[1] == '':
        var_dict['standardized_name'] += '_'

    return var_dict


def standardize_odf_units(unit_string):
    """
    Units strings were manually written within the ODF files.
    We're trying to standardize all the different issues found.
    """

    unit_string = unit_string.replace('**', '^')
    unit_string = unit_string.replace('µ', 'u')
    unit_string = re.sub(r' /|/ ', '/', unit_string)
    unit_string = re.sub(r' \^|\^ ', '^', unit_string)

    if re.match(r'\(none\)|none|dimensionless', unit_string):
        unit_string = 'none'
    return unit_string


def global_attributes_from_header(odf_header):
    """
    Method use to define the standard global attributes from an ODF Header parsed by the read function.
    """
    global_attributes = {"project": odf_header["CRUISE_HEADER"]["CRUISE_NAME"],
                         "institution": odf_header["CRUISE_HEADER"]["ORGANIZATION"],
                         "history": json.dumps(odf_header["HISTORY_HEADER"], ensure_ascii=False, indent=False),
                         "comment": odf_header["EVENT_HEADER"].get("EVENT_COMMENTS", ''),
                         "header": json.dumps(odf_header, ensure_ascii=False, indent=False)}
    return global_attributes


def generate_variables_from_header(ds,
                                   odf_header,
                                   cdm_data_type,
                                   original_var_field='original_variable'):
    """
    Method use to generate metadata variables from the ODF Header to a xarray Dataset.
    """
    initial_variable_order = list(ds.keys())

    # General Attributes
    ds["institution"] = odf_header["CRUISE_HEADER"]["ORGANIZATION"]
    ds["cruise_name"] = odf_header["CRUISE_HEADER"]["CRUISE_NAME"]
    ds["cruise_id"] = odf_header["CRUISE_HEADER"].get("CRUISE_NUMBER", '')
    ds["chief_scientist"] = odf_header["CRUISE_HEADER"]["CHIEF_SCIENTIST"]
    ds['platform'] = odf_header["CRUISE_HEADER"]["PLATFORM"]

    # Time Variables
    if "SYTM_01" in ds.keys():
        if cdm_data_type == 'Profile':
            ds.coords["time"] = ds['SYTM_01'].min().values
            ds["time_precise"] = ds['SYTM_01']
            ds["time"].attrs[original_var_field] = 'min(SYTM_01)'
            ds["time_precise"].attrs[original_var_field] = 'SYTM_01'
        else:
            ds.coords["time"] = ds['SYTM_01']
            ds["time"].attrs[original_var_field] = 'SYTM_01'
    else:
        ds.coords["time"] = pd.to_datetime(odf_header["EVENT_HEADER"]["START_DATE_TIME"])
        ds["time"].attrs[original_var_field] = "EVENT_HEADER:START_DATE_TIME"
    ds["start_time"] = pd.to_datetime(odf_header["EVENT_HEADER"]["START_DATE_TIME"])
    ds["end_time"] = pd.to_datetime(odf_header["EVENT_HEADER"]["END_DATE_TIME"])

    ds["start_time"].attrs.update({original_var_field: "EVENT_HEADER:START_DATE_TIME",
                                   '_FillValue': odf_time_null_value})
    ds["end_time"].attrs.update({original_var_field: "EVENT_HEADER:END_DATE_TIME",
                                 '_FillValue': odf_time_null_value})

    # Coordinate variables
    if "LATD_01" in ds.keys():
        if cdm_data_type in ['Profile', 'TimeSeries']:
            ds.coords["latitude"] = ds["LATD_01"][0].values
            ds['latitude'].attrs[original_var_field] = 'LATD_01[0]'
            ds['latitude_precise'] = ds["LATD_01"]
            ds["latitude_precise"].attrs[original_var_field] = "LATD_01"
            ds["latitude_precise"].attrs.update({
                'units': 'degrees_north',
                'standard_name': 'latitude',
                '_FillValue': -99}
            )
        else:
            ds.coords["latitude"] = ds["LATD_01"]
            ds["latitude"].attrs[original_var_field] = "LATD_01"
    else:
        ds.coords["latitude"] = float(odf_header["EVENT_HEADER"]["INITIAL_LATITUDE"])
        ds["latitude"].attrs[original_var_field] = "EVENT_HEADER:INITIAL_LATITUDE"

    ds['latitude'].attrs.update({'units': 'degrees_north',
                                 'standard_name': 'latitude',
                                 '_FillValue': -99})
    if "LOND_01" in ds.keys():
        if cdm_data_type in ['Profile', 'TimeSeries']:
            ds.coords["longitude"] = ds["LOND_01"][0].values
            ds['longitude'].attrs[original_var_field] = 'LOND_01[0]'
            ds['longitude_precise'] = ds["LOND_01"]
            ds["longitude_precise"].attrs[original_var_field] = "LOND_01"
            ds["longitude_precise"].attrs.update({
                'units': 'degrees_east',
                'standard_name': 'longitude',
                '_FillValue': -999}
            )

        else:
            ds.coords["longitude"] = ds["LOND_01"]
            ds["longitude"].attrs[original_var_field] = "LOND_01"
    else:
        ds.coords["longitude"] = float(odf_header["EVENT_HEADER"]["INITIAL_LONGITUDE"])
        ds["longitude"].attrs[original_var_field] = "EVENT_HEADER:INITIAL_LONGITUDE"

    ds['longitude'].attrs.update({'units': 'degrees_east',
                                  'standard_name': 'longitude',
                                  '_FillValue': -999})
    # Depth
    if 'DEPH_01' in ds:
        ds.coords['depth'] = ds['DEPH_01']
        ds['depth'].attrs[original_var_field] = 'DEPH_01'
    elif "PRES_01" in ds:
        ds.coords['depth'] = (ds['PRES_01'].dims, -gsw.z_from_p(ds['PRES_01'], ds['latitude']))
        ds['depth'].attrs[original_var_field] = "-gsw.z_from_p(PRES_01,latitude)"
    if 'depth' in ds:
        ds['depth'].attrs.update({'units': 'm',
                                  'standard_name': 'depth',
                                  'positive': 'down'})

    # Reorder variables
    variable_list = [var for var in ds.keys() if var not in initial_variable_order]
    variable_list.extend(list(ds.coords.keys()))
    variable_list.extend(initial_variable_order)
    ds = ds[variable_list]
    return ds
