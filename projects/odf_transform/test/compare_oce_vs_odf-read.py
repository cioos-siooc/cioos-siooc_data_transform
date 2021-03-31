import glob
import json
import datetime

import pandas as pd

import cioos_data_transform.utils.odf as odf
import cioos_data_transform.utils.oce as oce

import warnings

"""
Small script use to compare the OCE method to read the data and the parsing tool available 
within the cioos-data-transform package.
"""

decimal_rounding_value = 5

odflist = glob.glob("./test_files/*.ODF")
fulllist = glob.glob("./test_files/*")

for f in odflist:
    # Do we have both JSON and ODF available
    if f not in fulllist or f + '.json' not in fulllist:
        print('Missing JSON File:' + f)
        break

    # #### OCE JSON METHOD ####
    # Load JSON
    with open(f + '.json', "r") as fjson:
        odf_json = json.load(fjson)

    # Retrieve ODF Original Variable Attributes by digging in OCE
    odf_json_variable_attributes = oce.get_odf_variable_attributes(odf_json['metadata'], prefix='original_')

    # Retrieve ODF Original Data (Combine OCE data and Flags)
    odf_json_data = oce.retrieve_odf_data_from_oce(odf_json['data'], odf_json['metadata'],
                                                   odf_json_variable_attributes, 'original_')

    # #### Read ODF directly with cioos_data_transform.utils.odf.read() ####
    try:
        odf_read_header, odf_read_df = odf.read(f)
    except ValueError:
        warnings.warn('Failed to read {0}'.format(f))
        continue

    # #### Compare both methods variables ####
    if len(odf_json_data) != len(odf_read_df.columns):
        print('')
    print(f)
    for var in odf_json_data:
        if len(odf_json_data[var]) != len(odf_read_df[var]):
            print(var + 'diffrent records available')

        compare_df = pd.DataFrame()
        compare_df['json'] = odf_json_data[var]
        compare_df['odf_read'] = odf_read_df[var]

        # Let's ignore the null_values for now
        compare_df = compare_df.dropna(axis='index')

        # Convert to
        # Review Float numerical data
        if odf_read_df[var].dtype in ['datetime64[ns]']:
            temp_time = (odf_read_df[var] - datetime.datetime(1970, 1, 1)).dt.total_seconds()
            if (compare_df['json'].round(decimal_rounding_value) != temp_time.round(decimal_rounding_value)).any():
                print(str(var) + ' dates are different')
        # Review numerical data
        elif compare_df['json'].dtype in ['float64', 'float', 'int', 'int64']:
            if ((compare_df['json']-compare_df['odf_read']).abs() > float('10E-'+str(decimal_rounding_value))).any():
                print(var + ' values are different between the two methods')
        else:
            print("NOT SURE WHAT TO DO: " + str(var) + '[' + str(compare_df['json'].dtype) + ']')
