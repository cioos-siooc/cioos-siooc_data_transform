import netCDF4 as nc
import gsw
import numpy as np


def add_standard_variables(filename):
    
    # Load NC Data Set and get Variable list
    dset = nc.Dataset(filename, 'r+')
    variable_list = dset.variables.keys() 

    # Variable naming convention used
    variable_name_convention = 'BODC'

    # Create functions to be use internally
    # Function to detect if variable is available
    def _has_variable(input_str, string_list):
        result = any(x in input_str for x in string_list)
        return result

    # Function to replace Nan or flagged values by value from a second vector
    def _fill_nan(x, y):
        x[np.isnan(x)] = y[np.isnan(x)]
        if np.size(x[:].mask) > 1:
            x[x[:].mask] = y[x[:].mask]
        return x

    # Function to create a new variable in a NetCDF4 environment with similar type and dimensions than given variables.
    def _create_new_empty_variable(dset, new_variable_name, similar_variables, long_name, standard_name, units):
        multiple_variables = 0
        
        # Make sure that all variable to merge are consistent if there's multiple in one data set
        for x in similar_variables:
            if _has_variable(x,  dset.variables.keys()):
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
            
            if not _has_variable(new_variable_name,  dset.variables.keys()):
                dset.createVariable(new_variable_name, variable_type, variable_dimensions)
            
            dset[new_variable_name].long_name = long_name
            dset[new_variable_name].standard_name = standard_name
            dset[new_variable_name].units = units
        
        return dset

    # Combine Temperature Variables
    combine_var_names = ['TEMPS901', 'TEMP902', 'TEMPS601', 'TEMP602', 'TEMPRTN1']  # , 'TEMPRTN2', 'TEMPST01', 'TEMPST02']

    if _has_variable(combine_var_names, variable_list):
        # Create New Variable
        new_variable = 'sea_water_temperature'
        _create_new_empty_variable(dset, new_variable, combine_var_names, 'Sea Water Temperature', 'sea_water_temperature', 'degC')
        var = dset[new_variable]

        if variable_name_convention == 'BODC':  # If use BODC convention for variable names
            # Data already in ITS-90 Convention
            if _has_variable('TEMPS901', variable_list):
                _fill_nan(var, dset.variables['TEMPS901'])
            if _has_variable('TEMPS902', variable_list):
                _fill_nan(var, dset.variables['TEMPS902'])
            
            # Convert IPTS-68 to ITS-90
            if _has_variable('TEMPS601', variable_list):
                # Convert Primary Temperature Sensor Data from IPTS-68 to ITS-90
                _fill_nan(var, gsw.t90_from_t68(dset.variables['TEMPS601'][:]))
            if _has_variable('TEMPS602', variable_list):
                # Convert Secondary Temperature Sensor Data from IPTS-68 to ITS-90
                _fill_nan(var, gsw.t90_from_t68(dset.variables['TEMPS602'][:]))
            
            # Older Standards which still needs to be defined
            # if _has_variable('TEMPRTN',variable_list):
            #     _fill_nan(var, dset.variables['TEMPRTN'])
            # if _has_variable('TEMPST1',variable_list):
            #     _fill_nan(var, dset.variables['TEMPST1'])
            # if _has_variable('TEMPST2',variable_list):
            #     _fill_nan(var, dset.variables['TEMPST2'])
    
    # Combine Salinity (sea_water_practical_salinity)
    combine_var_names = ['PSALST01', 'PSALST02', 'SSALST01', 'SSALST02', 'PSALBST01', 'PSALBST02', 'PSALBST1', 'PSALBST2', 'ODSDM021']

    if _has_variable(combine_var_names, variable_list):
        # Create New Variable
        new_variable = 'sea_water_practical_salinity'
        _create_new_empty_variable(dset, new_variable, combine_var_names, 'Sea Water Practical Salinity', 'sea_water_practical_salinity', '')
        var = dset[new_variable]

        if variable_name_convention == 'BODC':  # If use BODC convention for variable names
            # Data already in Practical Salinity unit
            if _has_variable('PSALST01', variable_list):
                _fill_nan(var, dset.variables['PSALST01'])
            if _has_variable('PSALST02', variable_list):
                _fill_nan(var, dset.variables['PSALST02'])
            if _has_variable('PSALBST01', variable_list):
                _fill_nan(var, dset.variables['PSALBST01'])
            if _has_variable('PSALBST02', variable_list):
                _fill_nan(var, dset.variables['PSALBST02'])
            if _has_variable('PSALBST1', variable_list):
                _fill_nan(var, dset.variables['PSALBST1'])
            if _has_variable('PSALBST2', variable_list):
                _fill_nan(var, dset.variables['PSALBST2'])
                
            # Data with Salinity in PPT convert to Practical Salinity
            if _has_variable('SSALST01', variable_list):  # Convert Primary Salinity Data from IPTS-68 to ITS-90
                _fill_nan(var, gsw.SP_from_SK(dset.variables['SSALST01'][:]))
            if _has_variable('SSALST02', variable_list):  # Convert Seconday Salinity Data from IPTS-68 to ITS-90
                _fill_nan(var, gsw.SP_from_SK(dset.variables['SSALST02'][:]))
            if _has_variable('ODSDM021', variable_list):  # Convert Secondary Salinity Data from IPTS-68 to ITS-90
                _fill_nan(var, gsw.SP_from_SK(dset.variables['ODSDM021'][:]))

    # Combine Depth (depth)
    combine_var_names = ['depth', 'PRESPR01', 'PRESPR02']

    if _has_variable(combine_var_names, variable_list):
        # Create New Variable
        new_variable = 'depth'
        _create_new_empty_variable(dset, new_variable, combine_var_names, 'Depth in meters', 'depth_below_sea_level_in_meters', 'm')
        var = dset[new_variable]

        if variable_name_convention == 'BODC':
            # Data already in Depth (m)
            if _has_variable('depth', variable_list):
                _fill_nan(var, dset.variables['depth'])
                
            # Convert Pressure to Pressure with TEOS-10 z_from_p tool
            # Convert Primary Pressure Data from dbar to m
            if (_has_variable('PRESPR01', variable_list) and _has_variable('latitude', variable_list)):
                _fill_nan(var, gsw.z_from_p(dset.variables['PRESPR01'][:], dset.variables['latitude'][:]))
                # Convert Secondary Pressure Data from dbar to m
            if (_has_variable('PRESPR02', variable_list) and _has_variable('latitude', variable_list)):
                _fill_nan(var, gsw.z_from_p(dset.variables['PRESPR02'][:], dset.variables['latitude'][:]))

    # Combine pressure (sea_water_pressure)
    combine_var_names = ['PRESPR01', 'PRESPR02', 'depth']

    if _has_variable(combine_var_names, variable_list):
        # Create New Variable
        new_variable = 'sea_water_pressure'
        _create_new_empty_variable(dset, new_variable, combine_var_names, 'Sea Water Pressure in dbar', 'sea_water_pressure', 'dbar')
        var = dset[new_variable]

        if variable_name_convention == 'BODC':  # If use BODC convention for variable names
            # ata already in Sea Pressure (dBar)
            if _has_variable('PRESPR01', variable_list):
                _fill_nan(var, dset.variables['PRESPR01'])
            if _has_variable('PRESPR02', variable_list):
                _fill_nan(var, dset.variables['PRESPR02'])
                
            # Convert Depth to Pressure with TEOS-10 p_from_z tool
            # Convert Primary Pressure Data from dbar to m
            if (_has_variable('depth', variable_list) and _has_variable('latitude', variable_list)):
                _fill_nan(var, gsw.p_from_z(-dset.variables['depth'][:], dset.variables['latitude'][:]))
                    
    # Save to NetCDF File
    dset.close()
