from ios_data_transform.OceanNcFile import MCtdNcFile
from ios_data_transform.OceanNcVar import OceanNcVar
from ios_data_transform.utils.utils import is_in, find_geographic_area, read_geojson
from datetime import datetime
from pytz import timezone


def write_bcsop_ncfile(filename, profile_id, sopdf, status):
    '''
    use data from pandas dataframe sopdf to write the data into a netcdf file
    author: Pramod Thupaki pramod.thupaki@hakai.org
    inputs:
        filename: output file name to be created in netcdf format
        sopdf: pandas dataframe with BCSOP data
    output:
        NONE
    '''
    out = MCtdNcFile()
    # write global attributes
    out.featureType = 'timeSeries'
    out.summary = 'The dataset consists of 12 coastal stations that have been monitored for several decades, the earliest commencing in 1914. There are gaps in the daily data due to weather conditions being too dangerous for sampling'
    out.title = 'BC Lightstation data'
    out.institution = 'Institute of Ocean Sciences, 9860 West Saanich Road, Sidney, B.C., Canada'
    out.infoUrl = 'https://open.canada.ca/data/en/dataset/719955f2-bf8e-44f7-bc26-6bd623e82884'
    out.cdm_profile_variables = 'time'  
    # write full original header, as json dictionary
    out.description = open('header.txt').readlines()
    # initcreate dimension variable
    out.nrec = int(len(sopdf.index))
    ncfile_var_list = []
    ncfile_var_list.append(OceanNcVar('str_id', 'country', None, None, None, 'Canada'))
    ncfile_var_list.append(OceanNcVar('str_id', 'project', None, None, None, 'British Columbia Shore station Observation Program (BCSOP)'))
    ncfile_var_list.append(OceanNcVar('str_id', 'contact_name', None, None, None, 'Peter Chandler'))
    ncfile_var_list.append(OceanNcVar('str_id', 'contact_email',None, None, None, 'peter.chandler@dfo-mpo.gc.ca'))
    ncfile_var_list.append(OceanNcVar('str_id', 'agency', None, None, None, 'Fisheries and Oceans Canada'))
    ncfile_var_list.append(OceanNcVar('str_id', 'instrument_type', None, None, None, 'Given this is a multi-decade time series the sampling instruments have changed over time. At present measurements are made with a YSI Pro30 multimeter.'))
    ncfile_var_list.append(OceanNcVar('lat', 'latitude', 'degrees_north', None, None, sopdf['latitude'].values[0]))
    ncfile_var_list.append(OceanNcVar('lon', 'longitude', 'degrees_east', None, None, sopdf['longitude'].values[0]))
    ncfile_var_list.append(OceanNcVar('profile', 'profile', None, None, None, profile_id))
    ncfile_var_list.append(OceanNcVar('str_id', 'status', None, None, None, status))
    try:
        obs_time = [datetime.strptime(d, "%m/%d/%Y") for d in sopdf['date'].values]
    except:
        obs_time = [datetime.strptime(d, "%Y-%m-%d") for d in sopdf['date'].values]
    obs_time_utc = [timezone('UTC').localize(date_obj) for date_obj in obs_time]
    ncfile_var_list.append(OceanNcVar('time', 'time', None, None, None, obs_time_utc, vardim=('time')))
    # go through channels and add each variable depending on type
    null_value = float('NaN')
    # add temperature variable 
    ncfile_var_list.append(OceanNcVar('temperature', 'TEMPTC01',
                            'deg C', sopdf['temperature'].min,
                            sopdf['temperature'].max, sopdf['temperature'].values, ncfile_var_list,
                            ('time'), null_value, conv_to_BODC=False))
    # add salinity variable
    ncfile_var_list.append(OceanNcVar('salinity', 'PSALPR01',
                            'PSS-78', sopdf['salinity'].min,
                            sopdf['salinity'].max, sopdf['salinity'].values, ncfile_var_list,
                            ('time'), null_value, conv_to_BODC=False))
    # attach variables to ncfileclass and call method to write netcdf file
    out.varlist = ncfile_var_list
    out.write_ncfile(filename)
    print("Finished writing file:", filename)
    return 1
