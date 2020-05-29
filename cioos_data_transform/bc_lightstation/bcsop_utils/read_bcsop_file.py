import pandas as pd
from zipfile import ZipFile


def read_lightstation_data(fileobj):
    locname = fileobj.readline().decode("UTF-8").strip().split(':')[0]
    locid = locname.lower().replace(' ','_')
    df = pd.read_csv(fileobj, skip_blank_lines=True, usecols=range(5), header=0,
                names=['date','salinity', 'temperature', 'latitude','longitude'], 
                na_values=['', '99.9'])
    # drop rows that have nan for date
    df.dropna(subset=['date'], inplace=True)
    return locid, df