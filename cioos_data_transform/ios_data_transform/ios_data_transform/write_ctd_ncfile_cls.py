import random
import json
from OceanNcFile import CtdNcFile
from OceanNcVar import OceanNcVar
from utils import is_in


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
    out = CtdNcFile()
# write global attributes
    out.featureType = 'profile'
    out.summary = 'IOS CTD datafile'
    out.title = 'IOS CTD profile'
    out.institution = 'Institute of Ocean Sciences, 9860 West Saanich Road, Sidney, B.C., Canada'
    out.cdm_profile_variables = ''
# write full original header, as json dictionary
    out.HEADER = json.dumps(ctdcls.get_complete_header(), ensure_ascii=False, indent=False)
# initcreate dimension variable
    out.nrec = int(ctdcls.FILE['NUMBER OF RECORDS'])
# add variable profile_id (dummy variable)
    ncfile_var_list = []
    profile_id = random.randint(1, 100000)
    ncfile_var_list.append(OceanNcVar('profile', 'profile', '', '', '', profile_id))
# add administration variables
    if 'COUNTRY' in ctdcls.ADMINISTRATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'country', '', '', '', ctdcls.ADMINISTRATION['COUNTRY'].strip()))
    if 'MISSION' in ctdcls.ADMINISTRATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'mission_id', '', '', '', ctdcls.ADMINISTRATION['MISSION'].strip()))
    else:
        ncfile_var_list.append(OceanNcVar('str_id', 'mission_id', '', '', '', ctdcls.ADMINISTRATION['CRUISE'].strip()))
    if 'SCIENTIST' in ctdcls.ADMINISTRATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'scientist', '', '', '', ctdcls.ADMINISTRATION['SCIENTIST'].strip()))
    if 'PROJECT' in ctdcls.ADMINISTRATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'project', '', '', '', ctdcls.ADMINISTRATION['PROJECT'].strip()))
    if 'AGENCY' in ctdcls.ADMINISTRATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'agency', '', '', '', ctdcls.ADMINISTRATION['AGENCY'].strip()))
    if 'PLATFORM' in ctdcls.ADMINISTRATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'platform', '', '', '', ctdcls.ADMINISTRATION['PLATFORM'].strip()))
# add instrument type
    if 'TYPE' in ctdcls.INSTRUMENT:
        ncfile_var_list.append(OceanNcVar('str_id', 'instrument_type', '', '', '', ctdcls.INSTRUMENT['TYPE'].strip()))
    if 'MODEL' in ctdcls.INSTRUMENT:
        ncfile_var_list.append(OceanNcVar('str_id', 'instrument_model', '', '', '', ctdcls.INSTRUMENT['MODEL'].strip()))
    if 'SERIAL NUMBER' in ctdcls.INSTRUMENT:
        ncfile_var_list.append(OceanNcVar('str_id', 'instrument_serial_number', '', '', '', ctdcls.INSTRUMENT['SERIAL NUMBER'].strip()))
# add locations variables
    ncfile_var_list.append(OceanNcVar('lat', 'latitude', 'degrees_north', '', '', ctdcls.LOCATION['LATITUDE']))
    ncfile_var_list.append(OceanNcVar('lon', 'longitude', 'degrees_east', '', '', ctdcls.LOCATION['LONGITUDE']))
    if 'GEOGRAPHIC AREA' in ctdcls.LOCATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'geographic_area', '', '', '', ctdcls.LOCATION['GEOGRAPHIC AREA'].strip()))
    if 'EVENT NUMBER' in ctdcls.LOCATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'event_number', '', '', '', ctdcls.LOCATION['EVENT NUMBER'].strip()))
# add time variable
    ncfile_var_list.append(OceanNcVar('time', 'time', '', '', '', ctdcls.date))
# go through CHANNELS and add each variable depending on type
    for i, channel in enumerate(ctdcls.CHANNELS['Name']):
        if is_in(['depth'], channel):
            ncfile_var_list.append(OceanNcVar('depth', 'depth',
                ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
                ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i]))
        elif is_in(['pressure'], channel):
            ncfile_var_list.append(OceanNcVar('pressure', 'pressure',
                ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
                ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i]))
        elif is_in(['temperature'], channel):
            ncfile_var_list.append(OceanNcVar('temperature', ctdcls.CHANNELS['Name'][i],
                ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
                ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i]))
        elif is_in(['salinity'], channel):
            ncfile_var_list.append(OceanNcVar('salinity', ctdcls.CHANNELS['Name'][i],
                ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
                ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i]))
        else:
            print(channel, 'not transferred to netcdf file !')
            # raise Exception('not found !!')

    # attach variables to ncfileclass and call method to write netcdf file
    out.varlist = ncfile_var_list
    out.write_ncfile(filename)
    print(filename)