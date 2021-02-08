import sys
sys.path.insert(0, '../../cioos-siooc_data_transform/cioos_data_transform/ios_data_transform/')
import numpy as np
import json
from ios_data_transform.OceanNcFile import CtdNcFile
from ios_data_transform.OceanNcVar import OceanNcVar
from ios_data_transform.utils.utils import is_in, release_memory, find_geographic_area, read_geojson
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
    # out.HEADER = json.dumps(data['metadata']['header'], ensure_ascii=False, indent=False)
    # initcreate dimension variable
    out.nrec = len(data['data']['scan'])
    # add variable profile_id (dummy variable)
    ncfile_var_list = []
    ncfile_var_list.append(OceanNcVar('str_id', 'filename', None, None, None, data['metadata']['filename'].split('/')[-1]))
    # add administration variables
    ncfile_var_list.append(OceanNcVar('str_id', 'country', None, None, None, 'Canada'))
    ncfile_var_list.append(OceanNcVar('str_id', 'institute', None, None, None, data['metadata']['institute']))
    ncfile_var_list.append(OceanNcVar('str_id', 'cruise_id', None, None, None, data['metadata']['cruiseNumber']))
    ncfile_var_list.append(OceanNcVar('str_id', 'scientist', None, None, None, data['metadata']['scientist']))
    ncfile_var_list.append(OceanNcVar('str_id', 'project', None, None, None, data['metadata']['cruise']))
    ncfile_var_list.append(OceanNcVar('str_id', 'platform', None, None, None, data['metadata']['ship']))
    ncfile_var_list.append(OceanNcVar('str_id', 'instrument_type', None, None, None, data['metadata']['type']+' '+data['metadata']['model']))
    ncfile_var_list.append(OceanNcVar('str_id', 'instrument_serial_number', None, None, None,data['metadata']['serialNumber']))
    # add locations variables
    ncfile_var_list.append(OceanNcVar('lat', 'latitude', 'degrees_north', None, None, data['metadata']['latitude']))
    ncfile_var_list.append(OceanNcVar('lon', 'longitude', 'degrees_east', None, None, data['metadata']['longitude']))
    ncfile_var_list.append(OceanNcVar('str_id', 'geographic_area', None, None, None, ''))
    event_id = '{}-{}'.format(data['metadata']['eventQualifier'], data['metadata']['eventNumber'])
    ncfile_var_list.append(OceanNcVar('str_id', 'event_number', None, None, None, event_id))
    # create unique ID for each profile
    profile_id = '{}-{}'.format(data['metadata']['cruiseNumber'], data['metadata']['eventNumber'], data['metadata']['eventQualifier'])
    # print('Profile ID:',profile_id)
    ncfile_var_list.append(OceanNcVar('profile', 'profile', None, None, None, profile_id))
    # pramod - someone should check this...
    date_obj = datetime.utcfromtimestamp(data['metadata']['startTime'])
    date_obj = date_obj.astimezone(timezone('UTC'))
    ncfile_var_list.append(OceanNcVar('time', 'time', None, None, None, [date_obj]))

    for i, var in enumerate(data['data'].keys()):
        # 
        # ***********   CREATE A FUNCTION TO CONVERT UNITS FROM DICTIONARY FORMAT TO PLAIN STRING   ************
        # ***********   DETERMINE BODC/GF3 CODE FROM THE UNITS AND VARIABLE NAME IN ODF FILE ******************* 
        # 
        null_value = np.nan
        if is_in(['depth'], var):
            ncfile_var_list.append(OceanNcVar('depth', 'depth',
                                              'meters', min(data['data'][var]),
                                              max(data['data'][var]), data['data'][var], ncfile_var_list, ('z'),
                                              null_value))
        elif is_in(['pressure'], var):
            ncfile_var_list.append(OceanNcVar('pressure', 'pressure',
                                              'dbar', min(data['data'][var]),
                                              max(data['data'][var]), data['data'][var], ncfile_var_list, ('z'),
                                              null_value))
        elif is_in(['temperature'], var):
            ncfile_var_list.append(OceanNcVar('temperature', 'temperature',
                                              'IPTS-68', min(data['data'][var]),
                                              max(data['data'][var]), data['data'][var], ncfile_var_list, ('z'),
                                              null_value))
        elif is_in(['salinity'], var):
            ncfile_var_list.append(OceanNcVar('salinity', 'salinity',
                                              'PSS-78', min(data['data'][var]),
                                              max(data['data'][var]), data['data'][var], ncfile_var_list, ('z'),
                                              null_value))
        else:
            pass
            # print(var, data['metadata']['units'][var], 'not transferred to netcdf file !')
# now actuallY write the information in CtdNcFile object to a netcdf file
    out.varlist = ncfile_var_list
    # print('Writing ncfile:',filename)
    out.write_ncfile(filename)



flist = glob.glob('./test_files/*.json')
for f in flist:
    with open(f, 'r') as fid:
        data = fid.read()
        data = json.loads(data)
    # parse file
    try:
        # print(f)
        write_ctd_ncfile('./temp/{}.nc'.format(f.split('/')[-1]), data)
    except Exception as e:
        print('***** ERROR***',f)
        print(e)
        pass




