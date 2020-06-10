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
from bcsop_utils import read_lightstation_data


datapath = '/home/pramod/data/IOS_lighthouse_data/'
with ZipFile(datapath+'DATA_-_Active_Sites.zip') as myzipfile:
    for f in myzipfile.namelist():
        if 'Daily_Sea_Surface_Temperature_and_Salinity' in f and '.csv' in f and 'french' not in f:
            loc, df = read_lightstation_data(myzipfile.open(f))
            print(loc)
            write_bcsop_ncfile('/home/pramod/temp/{}.nc'.format(loc), loc, df)



