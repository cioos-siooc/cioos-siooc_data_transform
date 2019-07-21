from netCDF4 import Dataset as ncdata
import numpy as np 
import datetime
import random
import json
#   print random.randint(1,101)

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
    setattr(ncfile, 'featureType', 'profile')
    setattr(ncfile, 'summary', 'IOS CTD datafile')
    setattr(ncfile, 'title', 'IOS CTD profile')
    setattr(ncfile, 'institution', 'Institute of Ocean Sciences, 9860 West Saanich Road, Sidney, B.C., Canada')
    setattr(ncfile, 'history', '')
    setattr(ncfile, 'infoUrl', '')
    setattr(ncfile, 'cdm_profile_variables', 'temperature, salinity')
# write location information
    # setattr(ncfile, 'LOCATION', json.dumps(ctdcls.LOCATION, indent=True))
    setattr(ncfile, 'IOS_HEADER', json.dumps(ctdcls.get_complete_header(), ensure_ascii=False, indent=4))
# write global attributed from IOS 'FILE' section and 'ADMINISTRATION' section
    # setattr(ncfile, 'ADMINISTRATION', json.dumps(ctdcls.ADMINISTRATION, indent=True))
    # for key in ctdcls.FILE.keys():
    #     if key[0] != '$':
    #         setattr(ncfile, key, ctdcls.FILE[key])
# write comments and remarks to global attributes. this comes at the end for aesthetics
    # setattr(ncfile, 'COMMENTS', json.dumps(ctdcls.COMMENTS, indent=True))
    # setattr(ncfile, 'REMARKS', json.dumps(ctdcls.REMARKS, indent=True))
# create dimensions
    ncfile.createDimension('z', int(ctdcls.FILE['NUMBER OF RECORDS']))
    ncfile.createDimension('nchar', 10)
# add variable profile_id (dummy variable)
    profile_id = random.randint(1, 100000)
    __add_var(ncfile, 'profile', 'profile', '', '', '', profile_id)
    var = ncfile.createVariable('mission_id', 'S1', ('nchar',))
    if 'MISSION' in ctdcls.ADMINISTRATION.keys():
        mission_id = ctdcls.ADMINISTRATION['MISSION'].strip()
    else:
        mission_id = ctdcls.ADMINISTRATION['CRUISE'].strip()
    var[0:len(mission_id)] = [c for c in mission_id]
# add locations variables
    __add_var(ncfile, 'lat', 'latitude', 'degrees_north', '', '', ctdcls.LOCATION['LATITUDE'])
    __add_var(ncfile, 'lon', 'longitude', 'degrees_east', '', '', ctdcls.LOCATION['LONGITUDE'])
# add time variable
    __add_var(ncfile, 'time', 'time', '', '', '', ctdcls.date)
# go through CHANNELS and add each variable depending on type
    for i, channel in enumerate(ctdcls.CHANNELS['Name']):
        if is_in(['depth'], channel):
            print channel, 'found depth'
            __add_var(ncfile, 'depth', 'depth',
            ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
            ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i])
        elif is_in(['pressure'], channel):
            __add_var(ncfile, 'pressure', 'pressure',
            ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
            ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i])
        elif is_in(['temperature'], channel):
            __add_var(ncfile, 'temperature', ctdcls.CHANNELS['Name'][i],
            ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
            ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i])
        elif is_in(['salinity'], channel):
            __add_var(ncfile, 'salinity', ctdcls.CHANNELS['Name'][i],
            ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
            ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i])
        else:
            print(channel, 'not transferred to netcdf file !')
            # raise Exception('not found !!')
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
    # print vartype, __get_bodc_code(vartype, varname, varunits)
    # print ncfile.variables.keys() #list of existing variables in ncfile
    if vartype == 'profile':
        var = ncfile.createVariable('profile', 'int32', ())
        setattr(var, 'cf_role', 'profile_id')
        var[:] = varval
    elif vartype == 'lat':
        var = ncfile.createVariable('latitude', 'float32', ())
        setattr(var, 'long_name', 'Latitude')
        setattr(var, 'standard_name', 'latitude')
        setattr(var, 'units', 'degrees_north')
        var[:] = varval
    elif vartype == 'lon':
        var = ncfile.createVariable('longitude', 'float32', ())
        setattr(var, 'long_name', 'Longitude')
        setattr(var, 'standard_name', 'latitude')
        setattr(var, 'units', 'degrees_east')
        var[:] = varval
    elif vartype == 'time':
        var = ncfile.createVariable('time', 'double', ())
        setattr(var, 'standard_name', 'time')
        setattr(var, 'long_name', 'time')
        setattr(var, 'units', 'seconds since 1970-01-01 00:00:00')
        dt = datetime.datetime.strptime(varval, '%Y/%m/%d %H:%M:%S.%f %Z')
        var[:] = (dt - datetime.datetime(1970, 1, 1)).total_seconds()
    elif vartype == 'depth':
        var = ncfile.createVariable('depth', 'float32', ('z'))
        setattr(var, 'ios_name', varname.strip())
        setattr(var, 'long_name', '')
        setattr(var, 'standard_name', '')
        setattr(var, 'units', varunits.strip())
        setattr(var, 'Maximum', float(varmax))
        setattr(var, 'Minimum', float(varmin))
        var[:] = np.asarray(varval, dtype=float)
    elif vartype == 'PRESPR01':
        var = ncfile.createVariable('pressure', 'float32', ('z'))
        setattr(var, 'ios_name', varname.strip())
        setattr(var, 'long_name', 'Pressure')
        setattr(var, 'standard_name', 'sea_water_pressure')
        setattr(var, 'units', varunits.strip())
        setattr(var, 'Maximum', float(varmax))
        setattr(var, 'Minimum', float(varmin))
        var[:] = np.asarray(varval, dtype=float)
    elif vartype == 'temperature':
        # print varname.strip(), varunits
        for i in range(4):
            bodc_code, bodc_units = __get_bodc_code(vartype, varname, varunits, i)
            # print bodc_code
            if bodc_code not in ncfile.variables.keys():
                var = ncfile.createVariable(bodc_code, 'float32', ('z'))
                break
        setattr(var, 'ios_name', varname.strip())
        setattr(var, 'long_name', 'Sea Water Temperature')
        setattr(var, 'standard_name', 'sea_water_temperature')
        setattr(var, 'units', bodc_units)
        setattr(var, 'Maximum', float(varmax))
        setattr(var, 'Minimum', float(varmin))
        var[:] = np.asarray(varval, dtype=float)
    elif vartype == 'salinity':
        # print varname, varunits
        for i in range(4): # will try to get a unique variable name at least 4 times
            bodc_code, bodc_units = __get_bodc_code(vartype, varname, varunits, i)
            if bodc_code not in ncfile.variables.keys():
                var = ncfile.createVariable(bodc_code, 'float32', ('z'))
                break
        setattr(var, 'ios_name', varname.strip())
        setattr(var, 'long_name', 'Sea Water Practical Salinity')
        setattr(var, 'standard_name', 'sea_water_practical_salinity')
        setattr(var, 'units', bodc_units)
        setattr(var, 'Maximum', float(varmax))
        setattr(var, 'Minimum', float(varmin))
        var[:] = np.asarray(varval, dtype=float)


def is_in(keywords, string):
    # simple function to check if any keyword is in string
    # convert string and keywords to upper case before checking
    return any([string.upper().find(z.upper()) >= 0 for z in keywords])


def __get_bodc_code(vartype, ios_varname, varunits, iter):
    """
    return the correct BODC code based on variable type, units and ios variable name
    author: Pramod Thupaki pramod.thupaki@hakai.org
    inputs:
        varname:
        vartype: list. [0] = vartype, [1]=instance details (primary/secondary etc)
        varunits:
    output:
        BODC code
    """
    bodc_code = ''
    if vartype == 'temperature':
        if is_in(['ITS90', 'ITS-90'], varunits):
            bodc_code = 'TEMPS9'; bodc_units = 'deg C'
        elif is_in(['IPTS-68', 'IPTS68'], varunits):
            bodc_code = 'TEMPS6'; bodc_units = 'deg C'
        else: # if varunits does not specify type of temperature
            print("Temperature type not defined", ios_varname, varunits)
            bodc_code = 'TEMPS'; bodc_units = 'deg C'
        bodc_code = '{}{:02d}'.format(bodc_code, iter+1)
    elif vartype == 'salinity':
        if is_in(['PSS-78'], varunits):
            bodc_code = "PSALST"; bodc_units = 'PSS-78'
        elif is_in(['ppt'], varunits):
            bodc_code = "PSALSTPPT"; bodc_units = 'PPT'
        else:
            raise Exception("Salinity type not defined", ios_varname, varunits, vartype)
        bodc_code = '{}{:02d}'.format(bodc_code, iter+1)
    return bodc_code, bodc_units
