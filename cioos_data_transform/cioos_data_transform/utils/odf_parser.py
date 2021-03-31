import re
import pandas as pd

"""

Code to read ODF files directly, written by Jessy

This code is not currently used by odf_transform but is here for reference

This method essentially apply the following steps:
 1. Read line by line an ODF header and distribute each lines in a list of list and dictionaries.
 2. Read all the data as a list of text
 3. Convert that list of data into a Pandas Dataframe with Pandas
 4. Convert each column of the pandas data frame to the matching format specified in the ODF associated PARAMETER_HEADER
 5. Convert this Pandas DataFrame an xarray
 6. Add all global and variable attributes available within the ODF header.
 7. Save to a NetCDF file.  
"""

"""Dictionary with the mapping of the odf types to python types"""
odf_dtypes = {'DOUB': 'float64', 'SING': 'float32', 'SYTM': 'float64', 'INTE': 'int32'}


def read(filename,
         header_end='-- DATA --',
         data_delimiter=r'\s+',
         quotechar='\'',
         parameter_section='PARAMETER_HEADER',
         output_column_name='CODE',
         variable_type='TYPE',
         odf_type_to_pandas=None
         ):
    """
    Read_odf
    Read_odf parse the odf format used by some DFO organisation to python list of diectionary format and
    pandas dataframe. Once converted, the output can easily be converted to netcdf format.

    read_odf is a simple tool that  parse the header metadata and data from an DFO ODF file to a list of dictionaries.
    :param filename: ODF file to read
    :param header_end: Expression used at the end of a ODF file header to define the end of the header and
     start of the data.
    :param data_delimiter: delimiter used by the odf separate the different data columns.
    :param quotechar: quote character to regroup string variables
    :param parameter_section: section of the ODF that describes each variable attributes
    :param output_column_name: variable attribute used to name each data columns
    :param variable_type: variable attribute that describe each variable type
    :param odf_type_to_pandas: dictionary which map odf types versus python(Pandas) types.
    :return:
    """
    # Default mapping of ODF to Pandas data type
    if odf_type_to_pandas is None:
        odf_type_to_pandas = {'DOUB': float, 'SING': float,
                              'SYTM': '\'%d-%b-%Y %H:%M:%S.%f\'',
                              'INTE': int}

    odf_date_format = [{'SYTM': {'regex': r'^\s*\'\d\d\-\w\w\w\-\d\d\d\d\s\d\d\:\d\d\:\d\d\.\d*\'\s*$',
                                 'datetime': '\'%d-%b-%Y %H:%M:%S.%f\''}}]

    metadata = {}
    line_count = 0
    with open(filename, 'r') as f:
        line = ''
        original_header = []
        # Read header one line at the time
        while not line.__contains__(header_end):
            line = f.readline()
            line_count = line_count + 1
            # Drop some characters that aren't useful
            line = re.sub(r'\n|,$', '', line)

            # Collect each original odf header lines
            original_header.append(line)

            # Detect the end of the header
            if re.match(header_end, line):
                # May also be stop by the while condition
                break

            # Sections
            if re.match(r'^\w', line):
                section = line.replace('\n', '').replace(',', '')
                if section not in metadata:
                    metadata[section] = [{}]
                else:
                    metadata[section].append({})

            # Dictionary type lines (key=value)
            elif re.match(r'^\s+.+=', line):  # Something=This
                dict_line = re.split(r'=', line, maxsplit=1)  # Make sure that only the first = is use to split
                dict_line = [re.sub(r'^\s+|\s+$', '', item) for item in dict_line]  # Remove trailing white spaces
                # if re.match(r'^\'\d\d\-\w\w\w\-\d\d\d\d\s\d\d\:\d\d\:\d\d\.\d*\'$',dict_line[1]): # Read Time
                #     dict_line[1] = dt.datetime.strptime(dict_line[1], '\'%d-%b-%Y %H:%M:%S.%f\'')
                if re.match(r'\'.*\'', dict_line[1]):  # Is delimited by double quotes, definitely a string
                    dict_line[1] = str(re.sub(r'\'', '', dict_line[1]))
                elif re.match(r'^([+-]?[1-9]\d*|0)$', dict_line[1]):  # Integer
                    dict_line[1] = int(dict_line[1])
                elif re.match(r'^[+-]?([0-9]*[.])?[0-9]*$', dict_line[1]):  # Float
                    dict_line[1] = float(dict_line[1])
                    # TODO we should add special cases for
                    #  lat/lon if special encoding (ex 58°34'13''N) (not sure if there's really data like that)

                # Add to the metadata as a dictionary
                metadata[section][-1][dict_line[0]] = dict_line[1]

            elif re.match(r'^\s+.+', line):  # Unknown line format (likely comments)
                # TODO this hasn't been tested yet I haven't try an example with not a dictionary like line
                metadata[section].append(line)
            else:
                assert RuntimeError, "Can't understand the line: " + line

    # Add original header lines to the metadata
    metadata['original_header'] = original_header

    # Read the rest with pandas directly
    data_raw = pd.read_csv(filename, delimiter=data_delimiter, quotechar=quotechar,
                           skiprows=line_count, header=None)

    # Simplify the single sections to a dictionary
    for section in metadata:
        if len(metadata[section]) == 1 and \
                type(metadata[section][0]) is dict:
            metadata[section] = metadata[section][0]

    # Rename data columns based on PARAMETER_HEADER attribute (default CODE)
    columns_name = [att[output_column_name] for att in metadata[parameter_section]]
    data_raw.columns = columns_name

    # Format columns dtypes based on the format specified in the header
    for parm in metadata[parameter_section]:
        if parm[variable_type] in odf_type_to_pandas:
            if parm[variable_type] in ['SYTM']:  # Convert to datetime
                # Rely on pd.to_datetime to do the right conversion
                data_raw[parm[output_column_name]] = pd.to_datetime(data_raw[parm[output_column_name]])
            else:  # string, float, integers
                data_raw[parm[output_column_name]] = data_raw[parm[output_column_name]].astype(
                    odf_type_to_pandas[parm[variable_type]])
        else:
            raise AttributeError('Unknown Data Format: ' + parm[output_column_name] + '(' + parm[variable_type] + ')')
    return metadata, data_raw