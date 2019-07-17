from netCDF4 import Dataset as ncdata
import numpy as np 

def write_ctd_ncfile(filename, ctdcls):
    '''
    use data and methods in ctdcls object to write the CTD data into a netcdf file
    author: Pramod Thupaki pramod.thupaki@hakai.org
    inputs:
        filename: output file name to be created in netcdf format
        ctdcls: ctd object. includes methods to read IOS format and stores data
    output:
        NONE
    '''
    ncfile = ncdata(filename, 'w', format='NETCDF3_CLASSIC')
    setattr(ncfile, 'type', 'IOS CTD datafile')
    setattr(ncfile, 'title', '')
    setattr(ncfile, 'history', ctdcls.filename)
# write location information
    setattr(ncfile, 'latitude', ctdcls.location['LATITUDE'])
    setattr(ncfile, 'longitude', ctdcls.location['LONGITUDE'])
    setattr(ncfile, 'start_time', ctdcls.date)
# write global attributed from IOS 'FILE' section
    for key in ctdcls.FILE.keys():
        if key[0] != '$':
            setattr(ncfile, key, ctdcls.FILE[key])
# write comments and remarks to global attributes. this comes at the end for aesthetics
    setattr(ncfile, 'ios_comments', ctdcls.COMMENTS)
    setattr(ncfile, 'ios_remarks', ctdcls.REMARKS)
# create dimensions
    ncfile.createDimension('station', 1)
    ncfile.createDimension('z', int(ctdcls.FILE['NUMBER OF RECORDS']))
# add locations variables
    __add_var(ncfile, 'lat', 'latitude', 'degrees_north', '', '', ctdcls.location['LATITUDE'])
    __add_var(ncfile, 'lon', 'longitude', 'degrees_east', '', '', ctdcls.location['LONGITUDE'])
# go through channels and add each variable depending on type
    for i, channel in enumerate(ctdcls.channels['Name']):
        if channel.upper().find('DEPTH') >= 0:
            __add_var(ncfile, 'depth', 'depth',
            ctdcls.channels['Units'][i], ctdcls.channels['Minimum'][i],
            ctdcls.channels['Maximum'][i], ctdcls.data[:, i])
        elif channel.upper().find('PRESSURE') >= 0:
            __add_var(ncfile, 'pressure', 'pressure',
            ctdcls.channels['Units'][i], ctdcls.channels['Minimum'][i],
            ctdcls.channels['Maximum'][i], ctdcls.data[:, i])
        elif channel.upper().find('TEMPERATURE') >= 0:
            __add_var(ncfile, 'temperature', ctdcls.channels['Name'][i],
            ctdcls.channels['Units'][i], ctdcls.channels['Minimum'][i],
            ctdcls.channels['Maximum'][i], ctdcls.data[:, i])
        elif channel.upper().find('SALINITY') >= 0:
            __add_var(ncfile, 'salinity', ctdcls.channels['Name'][i],
            ctdcls.channels['Units'][i], ctdcls.channels['Minimum'][i],
            ctdcls.channels['Maximum'][i], ctdcls.data[:, i])
    ncfile.close()

def __add_var(ncfile, vartype, varname, varunits, varmin, varmax, varval):
    """
    add variable to netcdf file using variables passed as inputs
    author: Pramod Thupaki pramod.thupaki@hakai.org
    input:
        ncfile: Dataset object where variables will be added
        vartype: variable type
        varname: nominal name of variable being passed. this can be IOS_dataname.
                can be used to determine BODC codes
        varunits: Units specifications from IOS file
        varmin: minimum value of variable as specified in IOS file
        varmax: maximum value of variable as specified in IOS file
    output:
        NONE
    """
    if vartype == 'lat':
        var = ncfile.createVariable('latitude', 'float32', ('station'))
        setattr(var, 'long_name', 'Latitude')
        setattr(var, 'standard_name', 'latitude')
        setattr(var, 'units','degrees_north')
        var[:] = varval
    elif vartype == 'lon':
        var = ncfile.createVariable('longitude', 'float32', ('station'))
        setattr(var, 'long_name', 'Longitude')
        setattr(var, 'standard_name', 'latitude')
        setattr(var, 'units', 'degrees_east')
        var[:] = varval
    elif vartype == 'depth':
        var = ncfile.createVariable('depth', 'float32', ('z'))
        setattr(var, 'ios_name', varname.strip())
        setattr(var, 'long_name', '')
        setattr(var, 'standard_name', '')
        setattr(var, 'units', varunits.strip())
        setattr(var, 'Maximum', float(varmax))
        setattr(var, 'Minimum', float(varmin))
        var[:] = np.asarray(varval, dtype=float)
    elif vartype == 'pressure':
        var = ncfile.createVariable('pressure', 'float32', ('z'))
        setattr(var, 'ios_name', varname.strip())
        setattr(var, 'long_name', 'Pressure')
        setattr(var, 'standard_name', 'sea_water_pressure')
        setattr(var, 'units', varunits.strip())
        setattr(var, 'Maximum', float(varmax))
        setattr(var, 'Minimum', float(varmin))
        var[:] = np.asarray(varval, dtype=float)
    elif vartype == 'temperature':
        var = ncfile.createVariable('temperature', 'float32', ('z'))
        setattr(var, 'ios_name', varname.strip())
        setattr(var, 'long_name', 'Sea Water Temperature')
        setattr(var, 'standard_name', 'sea_water_temperature')
        setattr(var, 'units', varunits.strip())
        setattr(var, 'Maximum', float(varmax))
        setattr(var, 'Minimum', float(varmin))
        var[:] = np.asarray(varval, dtype=float)
    elif vartype == 'salinity':
        var = ncfile.createVariable('salinity', 'float32', ('z'))
        setattr(var, 'ios_name', varname.strip())
        setattr(var, 'long_name', 'Sea Water Practical Salinity')
        setattr(var, 'standard_name', 'sea_water_practical_salinity')
        setattr(var, 'units', varunits.strip())
        setattr(var, 'Maximum', float(varmax))
        setattr(var, 'Minimum', float(varmin))
        var[:] = np.asarray(varval, dtype=float)


def __get_bodc_code():
    """
    return the correct BODC code based on variable type, units and ios variable name
    author: Pramod Thupaki pramod.thupaki@hakai.org
    inputs:
        varname:
        vartype:
        varunits:
    output:
        BODC code
    """
    pass