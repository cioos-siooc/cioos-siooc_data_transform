import netCDF4 as nc
import gsw
import numpy as np
import datetime


def _get_time_stamp():
    # Get now timestamp in UTC as a string
    time_stamp = datetime.datetime.utcnow().isoformat() + ' UTC'
    return time_stamp


def _has_variable(input_str: list, string_list: list) -> bool:
    # Function to detect if variable is available
    result = any(x in input_str for x in string_list)
    return result


def _fill_nan(x, y):
    # _fill_nan replace any NaN or flagged values in the array x by its correspondent in y

    # Make a copy of the input x
    x_in = np.copy(x)
    is_updated = False

    # Function to replace Nan or flagged values by value from a second vector
    x[np.isnan(x)] = y[np.isnan(x)]

    if np.size(x[:].mask) > 1:
        x[x[:].mask] = y[x[:].mask]

    # Compare input vs output and output True if changed
    if (x_in != x[:]).any():
        is_updated = True
    return x, is_updated


def _create_new_empty_variable(dset, new_variable_name, similar_variables, long_name, standard_name, units):
    # Function to create a new variable in a NetCDF4 environment with similar type and dimensions than given variables.
    multiple_variables = 0

    # Make sure that all variable to merge are consistent if there's multiple in one data set
    for x in similar_variables:
        if x in dset.variables.keys():
            # Find matching variable and create temporary variable based on the first match
            if multiple_variables != 1:
                temp_var = dset[x]
                multiple_variables = 1

            # Compare similar variables to make sure dimensions and shape are similar
            elif multiple_variables:
                if temp_var.dimensions != dset[x].dimensions:
                    print('dimensions are different between variables')
                if temp_var.shape != dset[x].shape:
                    print('shape is different between variables')

    # Create the new variable with NaN values
    if multiple_variables:
        variable_dimensions = temp_var.dimensions
        variable_type = temp_var.dtype

        if new_variable_name not in dset.variables.keys():
            dset.createVariable(new_variable_name, variable_type, variable_dimensions)

        dset[new_variable_name].long_name = long_name
        dset[new_variable_name].standard_name = standard_name
        dset[new_variable_name].units = units

    return dset


def add_standard_variables(filename):
    # TODO Add documentation to the function

    # Load NC Data Set and get Variable list
    dset = nc.Dataset(filename, 'r+')
    variable_list = dset.variables.keys()

    history_attribute = {_get_time_stamp(): 'Standardize ocean variables with add_standard_name tool'}

    # Variable naming convention used
    variable_name_convention = 'BODC'

    # Combine Temperature Variables
    combine_var_names = ['TEMPS901', 'TEMP902', 'TEMPS601', 'TEMP602',
                         'TEMPRTN1']  # , 'TEMPRTN2', 'TEMPST01', 'TEMPST02']

    if _has_variable(combine_var_names, variable_list):
        # Create New Variable
        new_variable = 'sea_water_temperature'
        _create_new_empty_variable(dset, new_variable, combine_var_names, 'Sea Water Temperature',
                                   'sea_water_temperature', 'degC')
        var = dset[new_variable]
        # Define History
        add_history_line = 'Create ' + new_variable + ' variable and apply the following parameters: '

        if variable_name_convention == 'BODC':  # If use BODC convention for variable names
            # Data already in ITS-90 Convention
            if 'TEMPS901' in variable_list:
                var, is_updated = _fill_nan(var, dset.variables['TEMPS901'])
                if is_updated:
                    add_history_line = add_history_line + 'TEMPS901, '
            if 'TEMPS902' in variable_list:
                var, is_updated = _fill_nan(var, dset.variables['TEMPS902'])
                if is_updated:
                    add_history_line = add_history_line + 'TEMPS902, '

            # Convert IPTS-68 to ITS-90
            if 'TEMPS601' in variable_list:
                # Convert Primary Temperature Sensor Data from IPTS-68 to ITS-90
                var, is_updated = _fill_nan(var, gsw.t90_from_t68(dset.variables['TEMPS601'][:]))
                if is_updated:
                    add_history_line = add_history_line + 'TEOS-10 gsw.t90_from_t68(\'TEMPS601\'), '
            if 'TEMPS602' in variable_list:
                # Convert Secondary Temperature Sensor Data from IPTS-68 to ITS-90
                var, is_updated = _fill_nan(var, gsw.t90_from_t68(dset.variables['TEMPS602'][:]))
                if is_updated:
                    add_history_line = add_history_line + 'TEOS-10 gsw.t90_from_t68(\'TEMPS602\'), '

            # TODO: Add other Temperature variables to the sea_water_temperature, need to discuss transformations to apply first
            # Older Standards which still needs to be define
            # if 'TEMPRTN' in variable_list:
            #     var, is_updated = _fill_nan(var, dset.variables['TEMPRTN'])
            #     if is_updated:
            #       add_history_line = add_history_line + 'TEMPRTN, '
            # if 'TEMPST1' in variable_list:
            #     var, is_updated = _fill_nan(var, dset.variables['TEMPST1'])
            #     if is_updated:
            #       add_history_line = add_history_line + 'TEMPST1, '
            # if 'TEMPST2' in variable_list:
            #     var, is_updated = _fill_nan(var, dset.variables['TEMPST2'])
            #     if is_updated:
            #       add_history_line = add_history_line + 'TEMPST2, '

        # Append list of variables added to history_attribute
        history_attribute[_get_time_stamp()] = add_history_line[:-2]  # ignore the last ','
        dset[new_variable].comment = str({_get_time_stamp(): add_history_line[:-2]})

    # Combine Salinity (sea_water_practical_salinity)
    combine_var_names = ['PSALST01', 'PSALST02', 'SSALST01', 'SSALST02', 'PSALBST01', 'PSALBST02', 'PSALBST1',
                         'PSALBST2', 'ODSDM021']

    if _has_variable(combine_var_names, variable_list):
        # Create New Variable
        new_variable = 'sea_water_practical_salinity'
        _create_new_empty_variable(dset, new_variable, combine_var_names, 'Sea Water Practical Salinity',
                                   'sea_water_practical_salinity', 'PSS-78')
        var = dset[new_variable]

        # Define History
        add_history_line = 'Create ' + new_variable + ' variable and apply the following parameters: '

        if variable_name_convention == 'BODC':  # If use BODC convention for variable names
            # Data already in Practical Salinity unit
            if 'PSALST01' in variable_list:
                var, is_updated = _fill_nan(var, dset.variables['PSALST01'])
                if is_updated:
                    add_history_line = add_history_line + 'PSALST01, '
            if 'PSALST02' in variable_list:
                var, is_updated = _fill_nan(var, dset.variables['PSALST02'])
                if is_updated:
                    add_history_line = add_history_line + 'PSALST02, '
            if 'PSALBST01' in variable_list:
                var, is_updated = _fill_nan(var, dset.variables['PSALBST01'])
                if is_updated:
                    add_history_line = add_history_line + 'PSALBST01, '
            if 'PSALBST02' in variable_list:
                var, is_updated = _fill_nan(var, dset.variables['PSALBST02'])
                if is_updated:
                    add_history_line = add_history_line + 'PSALBST02, '
            if 'PSALBST1' in variable_list:
                var, is_updated = _fill_nan(var, dset.variables['PSALBST1'])
                if is_updated:
                    add_history_line = add_history_line + 'PSALBST1, '
            if 'PSALBST2' in variable_list:
                var, is_updated = _fill_nan(var, dset.variables['PSALBST2'])
                if is_updated:
                    add_history_line = add_history_line + 'PSALBST2, '

            # Data with Salinity in PPT convert to Practical Salinity
            if 'SSALST01' in variable_list:  # Convert Primary Salinity Data from IPTS-68 to ITS-90
                var, is_updated = _fill_nan(var, gsw.SP_from_SK(dset.variables['SSALST01'][:]))
                if is_updated:
                    add_history_line = add_history_line + 'TEOS-10 gsw.SP_from_SK(\'PSALBST2\'), '
            if 'SSALST02' in variable_list:  # Convert Seconday Salinity Data from IPTS-68 to ITS-90
                var, is_updated = _fill_nan(var, gsw.SP_from_SK(dset.variables['SSALST02'][:]))
                if is_updated:
                    add_history_line = add_history_line + 'TEOS-10 gsw.SP_from_SK(\'SSALST02\'), '
            if 'ODSDM021' in variable_list:  # Convert Secondary Salinity Data from IPTS-68 to ITS-90
                var, is_updated = _fill_nan(var, gsw.SP_from_SK(dset.variables['ODSDM021'][:]))
                if is_updated:
                    add_history_line = add_history_line + 'TEOS-10 gsw.SP_from_SK(\'ODSDM021\'), '

        # Append list of variables added to history_attribute for global attribute and comment to the varaible attribute
        history_attribute[_get_time_stamp()] = add_history_line[:-2]  # ignore the last ','
        dset[new_variable].comment = str({_get_time_stamp(): add_history_line[:-2]})

    # Combine Depth (depth)
    combine_var_names = ['PRESPR01', 'PRESPR02']

    if _has_variable(combine_var_names, variable_list):
        # Create New Variable
        new_variable = 'depth'
        _create_new_empty_variable(dset, new_variable, combine_var_names, 'Depth in meters',
                                   'depth_below_sea_level_in_meters', 'm')
        var = dset[new_variable]

        # Define History: Start opposite for this one since depth variable already exist
        add_history_line = ''

        if variable_name_convention == 'BODC':
            # Depth already in depth (m) if depth already existed

            # Convert Pressure to Depth with TEOS-10 z_from_p tool
            # Convert Primary Pressure Data from dbar to m
            if 'PRESPR01' in variable_list and 'latitude' in variable_list:
                var, is_updated = _fill_nan(var, -gsw.z_from_p(dset.variables['PRESPR01'][:],
                                                               dset.variables['latitude'][:]))
                if is_updated:
                    add_history_line = add_history_line + 'TEOS-10 gsw.z_from_p(\'PRESPR01\',\'latitude\'), '
            # Convert Secondary Pressure Data from dbar to m
            if 'PRESPR02' in variable_list and 'latitude' in variable_list:
                var, is_updated = _fill_nan(var, -gsw.z_from_p(dset.variables['PRESPR02'][:],
                                                               dset.variables['latitude'][:]))
                if is_updated:
                    add_history_line = add_history_line + 'TEOS-10 -gsw.z_from_p(\'PRESPR02\',\'latitude\'), '

            # TODO: Copy instrument_depth variable value to depth variable if no other parameters are available

            if len(add_history_line) > 1:
                add_history_line = 'Create ' + new_variable + ' variable and apply the following parameters: ' \
                                   + add_history_line
                # Append list of variables added to history_attribute
                history_attribute[_get_time_stamp()] = add_history_line[:-2]  # ignore the last ','
                dset[new_variable].comment = str({_get_time_stamp(): add_history_line[:-2]})

    # Combine pressure (sea_water_pressure)
    combine_var_names = ['PRESPR01', 'PRESPR02', 'depth']

    if _has_variable(combine_var_names, variable_list):
        # Create New Variable
        new_variable = 'sea_water_pressure'
        _create_new_empty_variable(dset, new_variable, combine_var_names, 'Sea Water Pressure in dbar',
                                   'sea_water_pressure', 'dbar')
        var = dset[new_variable]
        add_history_line = 'Create ' + new_variable + ' variable and apply the following parameters: '

        if variable_name_convention == 'BODC':  # If use BODC convention for variable names
            # Data already in Sea Pressure (dBar)
            if 'PRESPR01' in variable_list:
                var, is_updated = _fill_nan(var, dset.variables['PRESPR01'])
                if is_updated:
                    add_history_line = add_history_line + 'PRESPR01, '
            if 'PRESPR02' in variable_list:
                var, is_updated = _fill_nan(var, dset.variables['PRESPR02'])
                if is_updated:
                    add_history_line = add_history_line + 'PRESPR02, '

            # Convert Depth to Pressure with TEOS-10 p_from_z tool
            # Convert Primary Pressure Data from dbar to m
            if 'depth' in variable_list and 'latitude' in variable_list:
                var, is_updated = _fill_nan(var, gsw.p_from_z(-dset.variables['depth'][:],
                                                              dset.variables['latitude'][:]))
                if is_updated:
                    add_history_line = add_history_line + 'TEOS-10 gsw.p_from_z(-\'depth\',\'latitude\'), '

        # Append list of variables added to history_attribute
        history_attribute[_get_time_stamp()] = add_history_line[:-2]  # ignore the last ','
        dset[new_variable].comment = str({_get_time_stamp(): add_history_line[:-2]})

    # Add to history global attribute of the NetCDF
    try:
        dset.history = dset.history + str(history_attribute)
    except AttributeError:
        dset.history = str(history_attribute)

    # Save to NetCDF File
    dset.close()

# TODO Create tools to add derived variables, particularly sound speed, density, potential density etc.
