import xarray as xr
import pandas as pd
import datetime as dt
import re
import json

def add_variables_from_dict(ds,
                            config,
                            dim,
                            dictionary=None,
                            time='time',
                            latitude='latitude',
                            longitude='longitude',
                            depth='depth',
                            global_attribute=False):

    """
    This function adds new variables to an xarray dataset based from the configuration provided.
    It can retrieve data from a nested dictionary.
    """

    # Loop through each metadata fields listed in config
    for var, info in config.items():

        # If metadata in header dig in the header to find it
        if dictionary and 'dictionary' in info:
            look_in = dictionary
            for key in info['dictionary']:
                if key in look_in:
                    look_in = look_in[key]
                else:
                    raise RuntimeError('{0} key does not exist in {1}: {2}'.format(key, var, info['dictionary']))
            value = look_in
            if type(value) == dict:
                raise RuntimeError('{0} is does not match a single value.'.format(var))
        else:
            value = info

        # If a format is specified convert the data
        if 'format' in info:
            if info['format'].startswith('datetime'):
                # Use specified format
                # Convert the datetime based on the format provided
                if re.search(r'\[(.*)\]', info['format']):
                    value = dt.datetime.strptime(value, re.search(r'\[(.*)\]', info['format']).group(0)[1:-1])
                else:
                    # Try with Pandas
                    value = pd.to_datetime(value)
            elif info['format'] == 'json':
                value = json.dumps(value, ensure_ascii=False, indent=False)
            else:
                value = pd.Series(value).astype(info['format']).values[0]

        # Add variable to xarray dataset
        if global_attribute:
            ds.attrs[var] = value
        elif var in [time, latitude, longitude, depth]:
            ds.coords[var] = value
        else:
            ds[var] = value

    return ds

