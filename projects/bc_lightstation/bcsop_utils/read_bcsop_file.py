import pandas as pd
from zipfile import ZipFile
import numpy as np


def read_daily_data(fileobj):
    locname = fileobj.readline().decode("UTF-8").strip().split(':')[0].strip()
    locid = locname.lower().replace(' ','_')
    df = pd.read_csv(fileobj, skip_blank_lines=True, usecols=range(5), header=0,
                names=['date','salinity', 'temperature', 'latitude','longitude'], 
                na_values=['', '99.9','99.999','99.99'])
    # drop rows that have nan for date
    df.dropna(subset=['date'], inplace=True)
    return locid, df

def read_monthly_avg(fileobj, varname):
    locname = fileobj.readline().decode("UTF-8").strip().split(':')[0].strip()
    locid = locname.lower().replace(' ','_')
    df = pd.read_csv(fileobj, skip_blank_lines=True, usecols=range(13), header='infer',
                names='YEAR,JAN,FEB,MAR,APR,MAY,JUN,JUL,AUG,SEP,OCT,NOV,DEC'.split(','), 
                na_values=['', '99.9','99.999','99.99'])
    # drop rows that have nan for date
    df.dropna(subset=['YEAR'], inplace=True)
    # convert the dataframe to matrix
    data = df.values
    daterange = pd.date_range(start=f'{int(data[0,0])}-01-15', periods=data.shape[0]*12, freq = pd.tseries.offsets.SemiMonthBegin(n=2))
    # print(data[0,0],daterange)
    dtstr = [d.strftime('%Y-%m-%d') for d in daterange]
    val = np.ravel(data[:,1:], order='C')
    # print(val)
    df_new = pd.DataFrame(data={'date':dtstr, varname:val})
    df_new.dropna(subset=['date'], inplace=True)
    return locid, df_new