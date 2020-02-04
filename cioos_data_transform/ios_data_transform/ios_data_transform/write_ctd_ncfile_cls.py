# import random
import json
from .OceanNcFile import CtdNcFile
from .OceanNcVar import OceanNcVar
from .utils import is_in, release_memory, find_geographic_area, read_geojson
from datetime import datetime
from netCDF4 import Dataset as ncdata 
from shapely.geometry import Point


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
    if ctdcls.type == 'ctd':
        out.summary = 'This dataset contains observations made by the Institute of Ocean Sciences of Fisheries and Oceans (DFO) using CTDs mounted on rosettes.'
        out.title = 'This dataset contains observations made by the Institute of Ocean Sciences of Fisheries and Oceans (DFO) using CTDs mounted on rosettes.'
    elif ctdcls.type == 'bot':
        out.summary = 'This dataset contains observations made by the Institute of Ocean Sciences of Fisheries and Oceans (DFO) using water samples.'
        out.title = 'This dataset contains observations made by the Institute of Ocean Sciences of Fisheries and Oceans (DFO) using water samples.'
    else:
        raise Exception('file type not identified !')
    out.institution = 'Institute of Ocean Sciences, 9860 West Saanich Road, Sidney, B.C., Canada'
    out.infoUrl = 'http://www.pac.dfo-mpo.gc.ca/science/oceans/data-donnees/index-eng.html'
    out.cdm_profile_variables = 'time' # TEMPS901, TEMPS902, TEMPS601, TEMPS602, TEMPS01, PSALST01, PSALST02, PSALSTPPT01, PRESPR01
# write full original header, as json dictionary
    out.HEADER = json.dumps(ctdcls.get_complete_header(), ensure_ascii=False, indent=False)
# initcreate dimension variable
    out.nrec = int(ctdcls.FILE['NUMBER OF RECORDS'])
# add variable profile_id (dummy variable)
    ncfile_var_list = []
    ncfile_var_list.append(OceanNcVar('str_id', 'filename', None, None, None, ctdcls.filename.split('/')[-1]))
# add administration variables
    if 'COUNTRY' in ctdcls.ADMINISTRATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'country', None, None, None, ctdcls.ADMINISTRATION['COUNTRY'].strip()))
    if 'MISSION' in ctdcls.ADMINISTRATION:
        mission_id = ctdcls.ADMINISTRATION['MISSION'].strip()
    else:
        mission_id = ctdcls.ADMINISTRATION['CRUISE'].strip()
    buf = mission_id.split('-')
    mission_id = '{:04d}-{:03d}'.format(int(buf[0]), int(buf[1]))
    ncfile_var_list.append(OceanNcVar('str_id', 'mission_id', None, None, None, mission_id))
    if 'SCIENTIST' in ctdcls.ADMINISTRATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'scientist', None, None, None, ctdcls.ADMINISTRATION['SCIENTIST'].strip()))
    if 'PROJECT' in ctdcls.ADMINISTRATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'project', None, None, None, ctdcls.ADMINISTRATION['PROJECT'].strip()))
    if 'AGENCY' in ctdcls.ADMINISTRATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'agency', None, None, None, ctdcls.ADMINISTRATION['AGENCY'].strip()))
    if 'PLATFORM' in ctdcls.ADMINISTRATION:
        ncfile_var_list.append(OceanNcVar('str_id', 'platform', None, None, None, ctdcls.ADMINISTRATION['PLATFORM'].strip()))
# add instrument type
    if 'TYPE' in ctdcls.INSTRUMENT:
        ncfile_var_list.append(OceanNcVar('str_id', 'instrument_type', None, None, None, ctdcls.INSTRUMENT['TYPE'].strip()))
    if 'MODEL' in ctdcls.INSTRUMENT:
        ncfile_var_list.append(OceanNcVar('str_id', 'instrument_model', None, None, None, ctdcls.INSTRUMENT['MODEL'].strip()))
    if 'SERIAL NUMBER' in ctdcls.INSTRUMENT:
        ncfile_var_list.append(OceanNcVar('str_id', 'instrument_serial_number', None, None, None, ctdcls.INSTRUMENT['SERIAL NUMBER'].strip()))
# add locations variables
    ncfile_var_list.append(OceanNcVar('lat', 'latitude', 'degrees_north', None, None, ctdcls.LOCATION['LATITUDE']))
    ncfile_var_list.append(OceanNcVar('lon', 'longitude', 'degrees_east', None, None, ctdcls.LOCATION['LONGITUDE']))
    ncfile_var_list.append(OceanNcVar('str_id', 'geographic_area', None, None, None, ctdcls.geo_code))
    if 'EVENT NUMBER' in ctdcls.LOCATION:
        event_id = ctdcls.LOCATION['EVENT NUMBER'].strip()
    else:
        print("Event number not found!"+ctdcls.filename)
        event_id = ctdcls.filename.split('-')[-1][:-4]
        print('Guessing ...', ctdcls.filename, '; event id = ', event_id)
    ncfile_var_list.append(OceanNcVar('str_id', 'event_number', None, None, None, event_id))
# add time variable
    profile_id = '{:04d}-{:03d}-{}'.format(int(buf[0]), int(buf[1]), event_id.zfill(4))
    # print(profile_id)
    ncfile_var_list.append(OceanNcVar('profile', 'profile', None, None, None, profile_id))
    ncfile_var_list.append(OceanNcVar('time', 'time', None, None, None, [ctdcls.start_dateobj]))
# go through CHANNELS and add each variable depending on type
    for i, channel in enumerate(ctdcls.CHANNELS['Name']):
        try:
            null_value = ctdcls.channel_details['Pad'][i]
        except Exception as e:
            print("Channel Details missing. Setting Pad value to ' ' ...")
            null_value = "' '"
        if is_in(['depth'], channel) and not is_in(['nominal'], channel):
            ncfile_var_list.append(OceanNcVar('depth', 'depth',
                ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
                ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i], ncfile_var_list, ('z'), null_value))
        elif is_in(['pressure'], channel):
            ncfile_var_list.append(OceanNcVar('pressure', 'pressure',
                ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
                ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i], ncfile_var_list, ('z'), null_value))
        elif is_in(['temperature'], channel) and not is_in(['flag', 'rinko', 'bottle'], channel):
            ncfile_var_list.append(OceanNcVar('temperature', ctdcls.CHANNELS['Name'][i],
                ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
                ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i], ncfile_var_list, ('z'), null_value))
        elif is_in(['salinity'], channel) and not is_in(['flag'], channel):
            ncfile_var_list.append(OceanNcVar('salinity', ctdcls.CHANNELS['Name'][i],
                ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
                ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i], ncfile_var_list, ('z'), null_value))
        elif is_in(['oxygen'], channel) and not is_in(['flag', 'bottle', 'rinko', 'temperature', 'current', 'isotope', 'saturation'], channel):
            ncfile_var_list.append(OceanNcVar('oxygen', ctdcls.CHANNELS['Name'][i],
                ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
                ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i], ncfile_var_list, ('z'), null_value))
        elif is_in(['conductivity'], channel):
            ncfile_var_list.append(OceanNcVar('conductivity', ctdcls.CHANNELS['Name'][i],
                ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
                ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i], ncfile_var_list, ('z'), null_value))
        #     Nutrients in bottle files
        elif is_in(['nitrate_plus_nitrite', 'silicate', 'phosphate'], channel) and not is_in(['flag'], channel):
            try:
                ncfile_var_list.append(OceanNcVar('nutrient', ctdcls.CHANNELS['Name'][i],
                    ctdcls.CHANNELS['Units'][i], ctdcls.CHANNELS['Minimum'][i],
                    ctdcls.CHANNELS['Maximum'][i], ctdcls.data[:, i], ncfile_var_list, ('z'), null_value))
            except Exception as e:
                print(e)
        else:
            print(channel, ctdcls.CHANNELS['Units'][i], 'not transferred to netcdf file !')
            # raise Exception('not found !!')

    # attach variables to ncfileclass and call method to write netcdf file
    out.varlist = ncfile_var_list
    out.write_ncfile(filename)
    print("Finished writing file:", filename, "\n")
    # release_memory(out)
    return 1
