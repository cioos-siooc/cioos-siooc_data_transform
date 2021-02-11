import sys
import os
sys.path.insert(0, '../../')
import numpy as np
import json
from odf_transform.odfCls import CtdNcFile, NcVar
# from ios_data_transform.OceanNcFile import CtdNcFile
# from ios_data_transform.OceanNcVar import OceanNcVar as NcVar
from ios_data_transform import is_in, find_geographic_area, read_geojson
from datetime import datetime
from netCDF4 import Dataset as ncdata
from shapely.geometry import Point
from pytz import timezone
import glob 


def write_ctd_ncfile(filename,odf_data):
    '''
    use data and methods in ctdcls object to write the CTD data into a netcdf file
    author: 
    inputs:
        filename: output file name to be created in netcdf format
        ctdcls: ctd object. includes methods to read IOS format and stores data
    output:
        NONE
    '''
    out = CtdNcFile()
    # write global attributes
    out.featureType = 'profile'
    out.summary = ''
    out.title = ''
    out.institution = data['metadata']['institute']
    out.infoUrl = 'http://www.pac.dfo-mpo.gc.ca/science/oceans/data-donnees/index-eng.html'
    out.cdm_profile_variables = 'time'
    # write full original header, as json dictionary
    out.header = json.dumps(data['metadata']['header'], ensure_ascii=False, indent=False)
    # initcreate dimension variable
    out.nrec = len(data['data']['scan'])
    # add variable profile_id (dummy variable)
    ncfile_var_list = []
    ncfile_var_list.append(NcVar('str_id', 'filename', None, data['metadata']['filename'].split('/')[-1]))
    # add administration variables
    ncfile_var_list.append(NcVar('str_id', 'country', None, 'Canada'))
    ncfile_var_list.append(NcVar('str_id', 'institute', None, data['metadata']['institute']))
    ncfile_var_list.append(NcVar('str_id', 'cruise_id', None, data['metadata']['cruiseNumber']))
    ncfile_var_list.append(NcVar('str_id', 'scientist', None, data['metadata']['scientist']))
    ncfile_var_list.append(NcVar('str_id', 'project', None, data['metadata']['cruise']))
    ncfile_var_list.append(NcVar('str_id', 'platform', None, data['metadata']['ship']))
    ncfile_var_list.append(NcVar('str_id', 'instrument_type', None, data['metadata']['type']+' '+data['metadata']['model']))
    ncfile_var_list.append(NcVar('str_id', 'instrument_serial_number', None,data['metadata']['serialNumber']))
    # add locations variables
    ncfile_var_list.append(NcVar('lat', 'latitude', 'degrees_north', data['metadata']['latitude']))
    ncfile_var_list.append(NcVar('lon', 'longitude', 'degrees_east', data['metadata']['longitude']))
    ncfile_var_list.append(NcVar('str_id', 'geographic_area', None, ''))
    event_id = '{}-{}'.format(data['metadata']['eventQualifier'], data['metadata']['eventNumber'])
    ncfile_var_list.append(NcVar('str_id', 'event_number', None, event_id))
    # create unique ID for each profile
    profile_id = '{}-{}'.format(data['metadata']['cruiseNumber'], data['metadata']['eventNumber'], data['metadata']['eventQualifier'])
    # print('Profile ID:',profile_id)
    ncfile_var_list.append(NcVar('profile', 'profile', None, profile_id))
    # pramod - someone should check this...
    date_obj = datetime.utcfromtimestamp(data['metadata']['startTime'])
    date_obj = date_obj.astimezone(timezone('UTC'))
    ncfile_var_list.append(NcVar('time', 'time', None, [date_obj]))

    for i, var in enumerate(data['data'].keys()):
        # 
        # ***********  TODO: CREATE A FUNCTION TO CONVERT UNITS FROM DICTIONARY FORMAT TO PLAIN STRING   ************
        # ***********  TODO: DETERMINE BODC/GF3 CODE FROM THE UNITS AND VARIABLE NAME IN ODF FILE ******************* 
        # 
        null_value = np.nan
        if is_in(['depth'], var):
            ncfile_var_list.append(NcVar(vartype = 'depth', varname = 'depth', varunits = 'meters', 
                                              varval = data['data'][var], varclslist = ncfile_var_list, 
                                              vardim = ('z'), varnull= null_value))
        elif is_in(['pressure'], var):
            ncfile_var_list.append(NcVar('pressure', 'pressure','dbar', 
                                              data['data'][var], ncfile_var_list, ('z'),
                                              null_value))
        elif is_in(['temperature'], var):
            ncfile_var_list.append(NcVar('temperature', 'temperature','IPTS-68', 
                                              data['data'][var], ncfile_var_list, ('z'),
                                              null_value))
        elif is_in(['salinity'], var):
            ncfile_var_list.append(NcVar('salinity', 'salinity', 'PSS-78', 
                                            data['data'][var], ncfile_var_list, ('z'),
                                            null_value))
        else:
            pass
            # print(var, data['metadata']['units'][var], 'not transferred to netcdf file !')
# now actuallY write the information in CtdNcFile object to a netcdf file
    out.varlist = ncfile_var_list
    # print(ncfile_var_list[0])
    # print('Writing ncfile:',filename)
    out.write_ncfile(filename)



flist = glob.glob('./test_files/*.json')
if not os.path.isdir('./temp/'):
    os.mkdir('./temp/')

for f in flist:
    with open(f, 'r') as fid:
        data = fid.read()
        data = json.loads(data)
    # parse file
    # try:
        # print(f)
    write_ctd_ncfile('./temp/{}.nc'.format(f.split('/')[-1]), data)
    # except Exception as e:
    #     print('***** ERROR***',f)
    #     print(e)
    #     pass




