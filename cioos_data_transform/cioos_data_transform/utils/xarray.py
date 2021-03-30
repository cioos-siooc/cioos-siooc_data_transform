import xarray as xr
import pandas as pd
import numpy as np
import warnings
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
    It can retrieve data from a nested dictionary by providing one, it can the header of a file format which contains
    a lot of informtation and was previously cnverted to a dictionary.

    The data can be converted to any ohter format.
    """

    # Loop through each metadata fields listed in config
    for var, info in config.items():

        # Retrieve value to add
        #  If dictionary in header dig in the provided dictionary to find it
        if dictionary and 'dictionary' in info:
            look_in = dictionary
            for key in info['dictionary']:
                if look_in and key in look_in:
                    look_in = look_in.get(key)
                else:
                    warnings.warn('{0} key does not exist in {1}: {2}'.format(key, var, info['dictionary']),
                                  RuntimeWarning)
            value = look_in
            if type(value) == dict and 'format' not in info and 'json' in info['format']:
                warnings.warn('{0} is does not match a single value.  A json output is then recommended.'.format(var),
                              RuntimeWarning)
        else:
            value = info

        # If a format is specified convert the data
        if 'format' in info:
            if info['format'].startswith('datetime'):
                # Use specified format
                # Convert the datetime based on the format provided
                if re.search(r'\[(.*)\]', info['format']):
                    try:
                        value = dt.datetime.strptime(value, re.search(r'\[(.*)\]', info['format']).group(0)[1:-1])
                    except ValueError:
                        warnings.warn('Failed to read date {0}: {1}. Will try to use pandas.to_datetime()'\
                                             .format(var, value), RuntimeWarning)
                        value = pd.to_datetime(value)
                else:
                    # Try with Pandas
                    value = pd.to_datetime(value)
            elif info['format'] == 'json':
                value = json.dumps(value, ensure_ascii=False, indent=False)
            else:
                value = np.array(value).astype(info['format'])

        # Add variable to xarray dataset
        if global_attribute:
            ds.attrs[var] = value
        elif var in [time, latitude, longitude, depth]:
            ds.coords[var] = value
        else:
            ds[var] = value

    return ds


import numpy as np


def convert_variables_to_erddap_format(ds):
    """
    convert_variables_to_erddap_format converts each variables within an xarray to an
    ERDDAP compatible/recommended format.
      - datetime (timezone aware or not) are converted to: seconds since 1970-01-01T00:00:00[Z]
      - Any objects (usually strings) are converted to |S
    """
    variables_to_review = list(ds.keys())
    variables_to_review.extend(ds.coords.keys())
    for var in variables_to_review:
        if ds[var].dtype not in [float, int, 'float64', 'float32', 'int64', 'int32']:
            # Convert Datetime to seconds since 1970-01-01
            if ds[var].dtype.name.startswith('datetime'):
                # Convert Datetime to seconds since 1970-01-01
                if 'units' in ds[var].attrs:
                    ds[var].attrs.pop('units')
                # Timezone aware data
                if 'tz' in ds[var].dtype.name:
                    timezone = 'Z'
                else:
                    timezone = ''

                # Format encoding output
                ds[var].encoding['units'] = 'seconds since 1970-01-01 00:00:00' + timezone

            else:
                # Should be a string
                ds[var] = ds[var].astype(str).str.encode('utf-8').astype("|S")
                ds[var].attrs['_Encoding'] = 'UTF-8'
    return ds


def get_spatial_coverage_attributes(ds,
                                    time='time',
                                    lat='latitude',
                                    lon='longitude',
                                    depth='depth',
                                    ):
    """
    This method generates the geospatial and time coverage attributes associated to an xarray dataset.
    """
    time_spatial_coverage = {}
    # time
    if time in ds:
        time_spatial_coverage.update({
            'time_coverage_start': str(ds[time].min().values),
            'time_coverage_end': str(ds[time].max().values),
            'time_coverage_duration': str((ds[time].max() - ds[time].min())
                                          .values / np.timedelta64(1, 's')) + ' seconds'
        })

    # lat/long
    if lat in ds and lon in ds:
        time_spatial_coverage.update({
            'geospatial_lat_min': ds[lat].min().values,
            'geospatial_lat_max': ds[lat].max().values,
            'geospatial_lon_min': ds[lon].min().values,
            'geospatial_lon_max': ds[lon].max().values
        })

    # depth coverage
    if depth in ds:
        time_spatial_coverage.update({
            'geospatial_vertical_min': ds[depth].min().values,
            'geospatial_vertical_max': ds[depth].max().values
        })

    # Add to global attributes
    ds.attrs.update(time_spatial_coverage)
    return ds

