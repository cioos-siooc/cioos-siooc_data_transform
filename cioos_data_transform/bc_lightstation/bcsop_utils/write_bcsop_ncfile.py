from ios_data_transform.OceanNcFile import MCtdNcFile
from ios_data_transform.OceanNcVar import OceanNcVar
from ios_data_transform.utils import is_in, release_memory, find_geographic_area, read_geojson
from datetime import datetime
from pytz import timezone


def write_bcsop_ncfile(filename, profile_id, sopdf):
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
    out.summary = 'This dataset contains observations made by the BCSOP program'
    out.title = 'This dataset contains observations made by the BCSOP program.'
    out.institution = 'Institute of Ocean Sciences, 9860 West Saanich Road, Sidney, B.C., Canada'
    out.infoUrl = 'https://open.canada.ca/data/en/dataset/719955f2-bf8e-44f7-bc26-6bd623e82884'
    out.cdm_profile_variables = 'time'  
    # write full original header, as json dictionary
    out.HEADER = open('header.txt').readlines()
    # initcreate dimension variable
    out.nrec = int(len(sopdf.index))
    # add variable profile_id (dummy variable)
    ncfile_var_list = []
    # profile_id = random.randint(1, 100000)
    # ncfile_var_list.append(OceanNcVar('str_id', 'filename', None, None, None, ctdcls.filename.split('/')[-1]))
    # add administration variables
    # if 'COUNTRY' in ctdcls.administration:
    ncfile_var_list.append(OceanNcVar('str_id', 'country', None, None, None, 'Canada'))
    ncfile_var_list.append(OceanNcVar('str_id', 'project', None, None, None, 'BCSOP'))
    ncfile_var_list.append(OceanNcVar('str_id', 'scientist', None, None, None, ''))
    ncfile_var_list.append(OceanNcVar('str_id', 'agency', None, None, None, 'D.F.O'))
    # if 'PLATFORM' in ctdcls.administration:
    #     ncfile_var_list.append(
    #         OceanNcVar('str_id', 'platform', None, None, None, ctdcls.administration['PLATFORM'].strip()))
    # add instrument type
    # if 'TYPE' in ctdcls.instrument:
    #     ncfile_var_list.append(
    ncfile_var_list.append(OceanNcVar('str_id', 'instrument_type', None, None, None, ''))
    # if 'MODEL' in ctdcls.instrument:
    #     ncfile_var_list.append(
    #         OceanNcVar('str_id', 'instrument_model', None, None, None, ctdcls.instrument['MODEL'].strip()))
    # if 'SERIAL NUMBER' in ctdcls.instrument:
    #     ncfile_var_list.append(OceanNcVar('str_id', 'instrument_serial_number', None, None, None,
    #                                       ctdcls.instrument['SERIAL NUMBER'].strip()))
    # if 'DEPTH' in ctdcls.instrument:
    #     ncfile_var_list.append(
    #         OceanNcVar('instr_depth', 'instrument_depth', None, None, None, float(ctdcls.instrument['DEPTH'])))
    # add locations variables
    ncfile_var_list.append(OceanNcVar('lat', 'latitude', 'degrees_north', None, None, sopdf['latitude'].values[0]))
    ncfile_var_list.append(OceanNcVar('lon', 'longitude', 'degrees_east', None, None, sopdf['longitude'].values[0]))
    # ncfile_var_list.append(OceanNcVar('str_id', 'geographic_area', None, None, None, ctdcls.geo_code))

    # if 'EVENT NUMBER' in ctdcls.location:
    #     event_id = ctdcls.location['EVENT NUMBER'].strip()
    # else:
    #     print("Event number not found!" + ctdcls.filename)
    #     event_id = '0000'
    # ncfile_var_list.append(OceanNcVar('str_id', 'event_number', None, None, None, event_id))
    # add time variable
    # profile_id = '{:04d}-{:03d}-{:04d}'.format(int(buf[0]), int(buf[1]), int(event_id))
    # print(profile_id)
    # timezone(date_string[0:3]).localize(date_obj)
    ncfile_var_list.append(OceanNcVar('profile', 'profile', None, None, None, profile_id))
    obs_time = [datetime.strptime(d, "%m/%d/%Y") for d in sopdf['date'].values]
    obs_time_utc = [timezone('UTC').localize(date_obj) for date_obj in obs_time]
    ncfile_var_list.append(OceanNcVar('time', 'time', None, None, None, obs_time_utc, vardim=('time')))
    # go through channels and add each variable depending on type
    null_value = float('NaN')
    # add temperature variable 
    ncfile_var_list.append(OceanNcVar('temperature', 'sea surface temperature',
                            'deg C', sopdf['temperature'].min,
                            sopdf['temperature'].max, sopdf['temperature'].values, ncfile_var_list,
                            ('time'), null_value))
    # add salinity variable
    ncfile_var_list.append(OceanNcVar('salinity', 'sea surface salinity',
                            'PSS-78', sopdf['salinity'].min,
                            sopdf['salinity'].max, sopdf['salinity'].values, ncfile_var_list,
                            ('time'), null_value))
    # attach variables to ncfileclass and call method to write netcdf file
    out.varlist = ncfile_var_list
    out.write_ncfile(filename)
    print("Finished writing file:", filename, "\n")
    return 1
