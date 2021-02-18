# main script to read and convert data from BC Lightstations to netcdf format
# Author:
# License:
# Version information:
import sys
import os
import ios_data_transform as iod
import pandas as pd
import numpy as np
from zipfile import ZipFile
from bcsop_utils import write_bcsop_ncfile
from bcsop_utils.read_bcsop_file import read_daily_data, read_monthly_avg
from ios_data_transform.utils import import_env_variables
import sys
import matplotlib.pyplot as plt


def readLocDict(fname):
    data = np.genfromtxt(fname, skip_header=2, usecols=(0,3,4), dtype=str, delimiter=',')
    locDict = {}
    for i, loc in enumerate(data[:,0]):
        locDict[loc.replace(' ','_').lower()] = {'lat':float(data[i,1]), 'lon':float(data[i,2])}
    return locDict

env = import_env_variables('./.env')
datapath = env['bcsop_raw_folder']
ncpath = env['bcsop_ncfile_folder']
locDict = readLocDict(datapath+'BC_Lightstations_and_other_Sample_Sites.csv')
# print(locDict)
## ===================== transform daily observations
for fname in ['DATA_-_Active_Sites.zip','DATA_-_Archived_Sites.zip','DATA_-_In-active_Sites.zip']:
    with ZipFile(datapath+fname) as myzipfile:
        for f in myzipfile.namelist():
            if 'Daily_Sea_Surface_Temperature_and_Salinity' in f and '.csv' in f and 'french' not in f:
                loc, df = read_daily_data(myzipfile.open(f))
                status = fname.split('_')[2].lower()
                print(loc, status)
                write_bcsop_ncfile(f'{ncpath}/daily/{loc}.nc', loc, df, status)

## ===================== transform monthly average data
for fname in ['DATA_-_Active_Sites.zip','DATA_-_Archived_Sites.zip','DATA_-_In-active_Sites.zip']:
    with ZipFile(datapath+fname) as myzipfile:
        for f in myzipfile.namelist():
            if 'Average_Monthly_Sea_Surface_Temp' in f and '.csv' in f and 'french' not in f:
                try:
                    loc, dfTemp = read_monthly_avg(myzipfile.open(f), varname='temperature')
                    loc, dfSalt = read_monthly_avg(myzipfile.open(f.replace('Temperatures','Salinities')), 
                                                varname='salinity')
                    df = pd.merge(dfTemp, dfSalt, on='date')
                except Exception as e:
                    print(e)
                status = fname.split('_')[2].lower()
                print(loc, status)
                try:
                    df['latitude'] = locDict[loc]['lat']
                    df['longitude'] =locDict[loc]['lon']
                    # print(df)
                    write_bcsop_ncfile(f'{ncpath}/monthly/{loc}.nc', loc, df, status)
                except Exception as e:
                    print(e)
                    print(f'***Could not convert information for: {loc}')
                    # sys.exit()
                
