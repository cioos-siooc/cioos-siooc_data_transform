import re
import datetime as dt
import numpy as np
import warnings
import pandas as pd
import json
import gsw

# Dictionary with the mapping of the odf types to python types
odf_dtypes = {'DOUB': 'float64', 'SING': 'float32', 'DOUBLE': 'float64',
              'SYTM': 'float64', 'INTE': 'int32', 'CHAR': str}


def read(filename,
         header_end='-- DATA --',
         data_delimiter=r'\s+',
         quotechar='\'',
         parameter_section='PARAMETER_HEADER',
         output_column_name='CODE',
         variable_type='TYPE',
         section_items_minimum_whitespaces=2,
         odf_type_to_pandas=None,
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
            a. Use defined separator  to distinguish columns (default '\s+').
            b. Convert each column of the pandas data frame to the matching format specified in
            the TYPE attribute of the ODF associated PARAMETER_HEADER

    read_odf is a simple tool that  parse the header metadata and data from an DFO ODF file to a list of dictionaries.
    :param filename: ODF file to read
    :param header_end: Expression used at the end of a ODF file header to define the end of the header and
     start of the data.
    :param data_delimiter: delimiter used by the odf separate the different data columns.
    :param quotechar: quote character to regroup string variables
    :param parameter_section: section of the ODF that describes each variable attributes
    :param output_column_name: variable attribute used to name each data columns
    :param variable_type: variable attribute that describe each variable type
    :param section_items_minimum_whitespaces: maximum amount of spaces prior to a section to be considered as a section
    :param odf_type_to_pandas: dictionary which map odf types versus python(Pandas) types.
    :return:
    """
    # Default mapping of ODF to Pandas data type
    if odf_type_to_pandas is None:
        odf_type_to_pandas = odf_dtypes

    odf_date_format = {'SYTM': {'regex': r'^\s*\'\d\d\-\w\w\w\-\d\d\d\d\s\d\d\:\d\d\:\d\d\.\d*\'\s*$',
                                'datetime': '%d-%b-%Y %H:%M:%S.%f'},
                       'header': {'regex': r'^\'\d\d\-\w\w\w\-\d\d\d\d\s\d\d\:\d\d\:\d\d\.\d*\'$',
                                  'datetime': '\'%d-%b-%Y %H:%M:%S.%f\''}
                       }

    def _convert_to_number(value):
        """ Simple method to try to convert values to integer or float."""
        try:  # Try Integer first
            output_value = int(value)
        except ValueError:
            try:  # Then try float
                output_value = float(value)
            except ValueError:  # If nothing works just keep it as is
                output_value = re.sub(r'^\s*|\s*$', '', value)
        return output_value

    def _strtrim(string_to_trim):
        return re.sub(r'^\s*|\s*$', '', string_to_trim)

    metadata = {}  # Start with an empty dictionary
    with open(filename, 'r', encoding=encoding_format) as f:
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
                # if re.match(r'^\'\d\d\-\w\w\w\-\d\d\d\d\s\d\d\:\d\d\:\d\d\.\d*\'$',dict_line[1]): # Read Time
                #     dict_line[1] = dt.datetime.strptime(dict_line[1], '\'%d-%b-%Y %H:%M:%S.%f\'')
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
        column_format = {}
        column_names = []
        not_converted_columns = {}
        for att in metadata[parameter_section]:
            if output_column_name not in att:
                att[output_column_name] = att['NAME']

            column_names.append(att[output_column_name])
            if att[variable_type] not in ['SYTM'] and not column_names[-1].startswith('SYTM'):
                column_format[att[output_column_name]] = odf_type_to_pandas[att[variable_type]]
            else:
                not_converted_columns[att[output_column_name]] = odf_type_to_pandas[att[variable_type]]

        # Read with Pandas
        data_raw = pd.read_csv(f, delimiter=data_delimiter, quotechar=quotechar, header=None,
                               names=column_names, dtype=column_format, encoding=encoding_format)

    # Make sure that there's the same amount of variables read versus what is suggested in the header
    if len(data_raw.columns) != len(metadata[parameter_section]):
        raise RuntimeError('{0} variables were detected in the data versus {1} in the header.' \
                           .format(len(data_raw.columns), len(metadata[parameter_section])))

    if not_converted_columns:
        # Parse Date/Time SYTM Variables
        for parm in not_converted_columns:
            try:
                data_raw[parm] = pd.to_datetime(data_raw[parm], format=odf_date_format['SYTM']['datetime'])
            except ValueError:
                warnings.warn('Failed to read SYTM variable. Will try to use pandas.to_datetime()', RuntimeWarning)
                data_raw[parm[output_column_name]] = pd.to_datetime(data_raw[parm[output_column_name]])
    return metadata, data_raw


def define_odf_variable_attributes(metadata,
                                   oce_metadata=None,
                                   organizations=None,
                                   vocabulary=None,
                                   vocabulary_attribute_list=None,
                                   odf_var_header_prefix='original_',
                                   odf_variable_name="CODE",
                                   flag_prefix='QualityFlag:'):
    """
    This method is use to retrieve from an ODF file each variable code and corresponding related
    vocabularies associated to the organization and variable name.
    Flag columns are also reviewed and matched to the appropriate variable.
    """
    # Make sure organization is a list
    if type(organizations) is str:
        organizations = [organizations]
    # Define vocabulary default list of variables to import as attributes
    if vocabulary_attribute_list is None:
        vocabulary_attribute_list = ['name', 'standard_name',
                                     'sdn_parameter_urn', 'sdn_parameter_name',
                                     'sdn_uom_urn', 'sdn_uom_name']
    parameter_code_att = odf_var_header_prefix + odf_variable_name

    def _find_previous_key(key_list, key):
        """
        For some ODF format, a flag column is related to the variable prior to it. This tool is use to retrieve
        the name of this variable situated prior to a given column.
        """
        previous_key = ''
        for i in key_list.keys():
            if i == key:
                break
            else:
                previous_key = i
        return previous_key

    # Find matching vocabulary
    if type(vocabulary) is dict:
        flag_dict = {}
        for var in metadata.keys():
            # Retrieve ODF CODE and Associated number
            odf_parameter_code = metadata[var][parameter_code_att]
            # Separate the parameter_code from the number at the end of the variable
            parameter_code = odf_parameter_code.rsplit('_', 1)

            # Retrieve trailing
            # Parameter codes are generally associated with a trailing number the define the
            # primary, secondary ,... data
            metadata[var]['parameter_code'] = parameter_code[0]
            if len(parameter_code) == 2 and parameter_code[1] != '':
                metadata[var]['parameter_code_number'] = int(parameter_code[1])
            else:
                metadata[var]['parameter_code_number'] = 1

            # FLAG VARIABLES Detect if it is a flag column associated with another column
            flag_column = False
            if parameter_code[0].startswith('QQQQ'):  # MLI FLAG should apply to previous variable
                flag_dict[odf_parameter_code] = _find_previous_key(metadata, var)
                flag_column = True
            elif parameter_code[0].startswith('Q') and odf_parameter_code[1:] in metadata.keys():
                # BIO Format which Q+[PCODE] of the associated variable
                flag_dict[odf_parameter_code] = odf_parameter_code[1:]
                flag_column = True
            # Make sure that the flag column relate to something
            if flag_column and flag_dict[odf_parameter_code] not in metadata:
                warnings.warn(odf_parameter_code + ' flag is refering to' + \
                              flag_dict[odf_parameter_code] + ' which is not available as variable',
                              UserWarning)

            # Loop through each organisations and find the matching parameter_code within the vocabulary
            found_matching_vocab = False
            for organization in organizations:
                if parameter_code[0] in vocabulary[organization]:
                    vocab_attributes = {key: value for key, value in vocabulary[organization][parameter_code[0]].items()
                                        if key in vocabulary_attribute_list}
                    metadata[var].update(vocab_attributes)
                    found_matching_vocab = True
                    break  # Stop after the first one detected

            # If will get there if no matching vocabulary exist
            if not found_matching_vocab and not flag_column:
                print(str(parameter_code) + ' not available in vocabularies: ' + str(organizations))

            # TODO compare expected units to units saved within the ODF file to make sure it is matching the vocabulary

    # Add Flag specific attributes
    for flag_column, data_column in flag_dict.items():
        if data_column in metadata:
            # Add long name attribute which is generally QUALITY_FLAG: [variable it affects]
            if 'name' in metadata[data_column]:
                metadata[flag_column]['long_name'] = flag_prefix + metadata[data_column]['name']
            else:
                metadata[flag_column]['long_name'] = flag_prefix + data_column

            # Add ancillary_variables attribute
            if 'ancillary_variables' not in metadata[data_column]:
                metadata[data_column]['ancillary_variables'] = flag_column
            elif 'ancillary_variables' in metadata[data_column] and type(metadata[data_column]) is str:
                metadata[data_column]['ancillary_variables'] += ',' + flag_column
            else:
                warnings.warn('unknown ancillary flag format attribute', UserWarning)
        # TODO improve flag parameters default documentation
        #  - add flag_values, flag_masks and flag_meanings to flag attributes
        #       http://cfconventions.org/cf-conventions/cf-conventions.html#flags
        #       we would need to know the convention used by the organization if there's any.
        #       Otherwise, this should be implemented within the erddap dataset.

    # Deal with fill value which are already specified within the ODF format
    for key, var in metadata.items():
        if 'original_NULL_VALUE' in var.keys():
            if var['original_NULL_VALUE'] not in ['', None, '(none)']:
                # Some ODF files used a FORTRAN Format (essentially replace the D in '-.99000000D+02' by E)
                if type(var['original_NULL_VALUE']) is str:
                    var['original_NULL_VALUE'] = re.sub('(?<=\d)D(?=[\+\-\d]\d)', 'E', var['original_NULL_VALUE'])

                if var['original_TYPE'] not in ['SYTM', 'INTE']:
                    null_value = np.array(var['original_NULL_VALUE']) \
                        .astype(odf_dtypes[var['original_TYPE']])
                elif var['original_TYPE'] == 'SYTM' and \
                        re.match(r'\d\d-\w\w\w-\d\d\d\d\s\d\d\:\d\d\:\d\d', var['original_NULL_VALUE']):
                    null_value = (dt.datetime.strptime(var['original_NULL_VALUE'],
                                                       '%d-%b-%Y %H:%M:%S.%f') - dt.datetime(1970, 1,
                                                                                             1)).total_seconds()
                elif var['original_TYPE'] == 'INTE':
                    null_value = int(np.array(var['original_NULL_VALUE']).astype(float).round())

                metadata[key]['null_value'] = null_value

    # Update P01 name based on parameter_code number
    for var in metadata:
        if 'sdn_parameter_urn' in metadata[var] and \
                type(metadata[var]['sdn_parameter_urn']) is str:
            metadata[var]['sdn_parameter_urn'] = re.sub(r'\d\d$',
                                                        '%02d' % metadata[var]['parameter_code_number'],
                                                        metadata[var]['sdn_parameter_urn'])
        if 'name' in metadata[var] and \
                type(metadata[var]['name']) is str:
            metadata[var]['name'] = re.sub(r'\d\d$',
                                           '%02d' % metadata[var]['parameter_code_number'],
                                           metadata[var]['name'])

    # TODO Add Warning for missing information and attributes (maybe)
    #  Example: Standard Name, P01, P02
    return metadata


def global_attributes_from_header(ds,
                                  odf_header):
    global_attributes = {"project": odf_header["CRUISE_HEADER"]["CRUISE_NAME"],
                         "institution": odf_header["CRUISE_HEADER"]["ORGANIZATION"],
                         "history": json.dumps(odf_header["HISTORY_HEADER"], ensure_ascii=False, indent=False),
                         "comment": odf_header["EVENT_HEADER"]["EVENT_COMMENTS"]
    }
    ds.attrs.update(global_attributes)

    # Copy the original header to a json dictionary
    ds.attrs["header"] = json.dumps(odf_header, ensure_ascii=False, indent=False)
    return ds


def generate_variables_from_header(ds,
                                   odf_header,
                                   cdm_data_type,
                                   date_format='%d-%b-%Y %H:%M:%S.%f',
                                   original_var_field='original_variable'):
    initial_variable_order = list(ds.keys())

    # General Attributes
    ds["institution"] = odf_header["CRUISE_HEADER"]["ORGANIZATION"]
    ds["cruise_name"] = odf_header["CRUISE_HEADER"]["CRUISE_NAME"]
    ds["cruise_id"] = odf_header["CRUISE_HEADER"]["CRUISE_NUMBER"]
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
        ds.coords["time"] = pd.to_datetime(odf_header["EVENT_HEADER"]["START_DATE_TIME"], format=date_format)
        ds["time"].attrs[original_var_field] = "EVENT_HEADER:START_DATE_TIME"
    ds["start_time"] = pd.to_datetime(odf_header["EVENT_HEADER"]["START_DATE_TIME"], format=date_format)
    ds["end_time"] = pd.to_datetime(odf_header["EVENT_HEADER"]["END_DATE_TIME"], format=date_format)

    # Coordinate variables
    if "LATD_01" in ds.keys():
        if cdm_data_type in ['Profile', 'TimeSeries']:
            ds.coords["latitude"] = ds["LATD_01"][0].values
            ds['latitude'].attrs[original_var_field] = 'LATD_01[0]'
            ds['latitude_precise'] = ds["LATD_01"]
            ds["latitude_precise"].attrs[original_var_field] = "LATD_01"
        else:
            ds.coords["latitude"] = ds["LATD_01"]
            ds["latitude"].attrs[original_var_field] = "LATD_01"
    else:
        ds.coords["latitude"] = float(odf_header["EVENT_HEADER"]["INITIAL_LATITUDE"])
        ds["latitude"].attrs[original_var_field] = "EVENT_HEADER:INITIAL_LATITUDE"
    if "LOND_01" in ds.keys():
        if cdm_data_type in ['Profile', 'TimeSeries']:
            ds.coords["longitude"] = ds["LOND_01"][0].values
            ds['longitude'].attrs[original_var_field] = 'LOND_01[0]'
            ds['longitude_precise'] = ds["LOND_01"]
            ds["longitude_precise"].attrs[original_var_field] = "LOND_01"
        else:
            ds.coords["longitude"] = ds["LOND_01"]
            ds["longitude"].attrs[original_var_field] = "LOND_01"
    else:
        ds.coords["longitude"] = float(odf_header["EVENT_HEADER"]["INITIAL_LONGITUDE"])
        ds["longitude"].attrs[original_var_field] = "EVENT_HEADER:INITIAL_LONGITUDE"

    # Depth
    if 'DEPH_01' in ds:
        ds.coords['depth'] = ds['DEPH_01']
        ds['depth'].attrs[original_var_field] = 'DEPH_01'
    elif "PRES_01" in ds:
        ds.coords['depth'] = (ds['PRES_01'].dims, -gsw.z_from_p(ds['PRES_01'], ds['latitude']))
        ds['depth'].attrs[original_var_field] = "-gsw.z_from_p(PRES_01,latitude)"

    # Reorder variables
    variable_list = [var for var in ds.keys() if var not in initial_variable_order]
    variable_list.extend(list(ds.coords.keys()))
    variable_list.extend(initial_variable_order)
    ds = ds[variable_list]
    return ds

