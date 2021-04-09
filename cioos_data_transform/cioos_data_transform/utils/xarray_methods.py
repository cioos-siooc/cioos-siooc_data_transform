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
    a lot of information and was previously converted to a dictionary.

    The data can be converted to any other format.
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
        elif 'variable' in info:
            if info['variable'] in ds:
                value = ds[info['variable']]
            else:
                break
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

        # Add attributes from config
        if 'attributes' in info:
            ds[var].attrs.update(info['attributes'])

    return ds


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
                ds[var] = ds[var].astype(str)  # .str.encode('utf-8')  # Force to encode in UTF-8
                # ds[var].attrs['_Encoding'] = 'UTF-8'
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
            'time_coverage_duration': pd.to_timedelta((ds[time].max() - ds[time].min()).values).isoformat()
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


def derive_cdm_data_type(ds,
                         cdm_data_type=None,
                         lat='latitude',
                         lon='longitude',
                         time='time',
                         depth='depth',
                         profile_id='profile_id',
                         timeseries_id='timeseries_id',
                         trajectory_id='trajectory_id'):

    if cdm_data_type is None:
        if lat in ds and lon in ds and \
                ds[lat].ndim == 1 and ds[lon].ndim == 1 and \
                ds[lat].size > 1 and ds[lon].size > 1:  # Trajectory
            is_trajectory = True
            cdm_data_type += 'Trajectory'
        else:
            is_trajectory = False

        if time in ds and ds[time].ndim == 1 and ds[time].size > 1 and not is_trajectory:  # Time Series
            cdm_data_type += 'TimeSeries'

        if depth in ds and ds[depth].ndim == 1 and ds[depth].size > 1:  # Profile
            cdm_data_type += 'Profile'

        if cdm_data_type == '':  # If nothing else
            cdm_data_type = 'Point'

    # Add cdm_data_type attribute
    ds.attrs['cdm_data_type'] = cdm_data_type

    def _retrieve_cdm_variables(ds_review,
                                var_id, type):
        if var_id not in ds:
            warnings('Missing a {0} variable'.format(type), RuntimeWarning)
            return ds_review

        ds[var_id].attrs['cf_role'] = type
        cdm_attribute = 'cdm_{0}_variables'.format(type.replace('_id', ''))
        if ds[var_id].size == 1:
            ds.attrs[cdm_attribute] = ','.join([var for var in ds_review if ds[var].size == 1])
        else:
            warnings('derive_cdm_data_type isn''t yet compatible with collection datasets', RuntimeWarning)
        return ds_review

    # Trajectory
    if 'Trajectory' in cdm_data_type:
        ds = _retrieve_cdm_variables(ds, trajectory_id, 'trajectory_id')

    # Time Series
    if 'Timeseries' in cdm_data_type :
        ds = _retrieve_cdm_variables(ds, timeseries_id)

    # Profile
    if 'Profile' in cdm_data_type:
        ds = _retrieve_cdm_variables(ds, profile_id, 'profile_id')
    return ds


def define_index_dimensions(ds):
    # If multiple dimensions exists but are really the same let's just keep index
    if 'index' in ds.dims and len(ds.dims) > 1:
        print('index')
    # Handle dimension name if still default "index" from conversion of pandas to xarray
    if 'index' in ds.dims and len(ds.dims.keys()) == 1:
        # If dimension is index and is a table like data
        if ds.attrs['cdm_data_type'] in ['Timeseries', 'Trajectory']:
            ds = ds.swap_dims({'index': 'time'})
            ds = ds.reset_coords('index')
        elif 'Profile' == ds.attrs['cdm_data_type']:
            ds = ds.swap_dims({'index': 'depth'})
            ds = ds.reset_coords('index')

    return ds


def add_variable_attributes(ds,
                            review_attributes=None,
                            scales='IPTS\-*48|IPTS\-*68|ITS\-*90|PSS\-*78'):
    if review_attributes is None:
        review_attributes = ['units', 'long_name', 'standard_name', 'comments', 'sdn_parameter_name']

    for var in ds:
        # Scale attribute
        if 'scale' not in ds[var].attrs:
            scale = []
            for att_to_review in review_attributes:
                if att_to_review in ds[var].attrs and len(scale) == 0:
                    scale = re.findall(scales.lower(), ds[var].attrs[att_to_review].lower())
                if scale:
                    ds[var].attrs['scale'] = scale[0].upper()
                    break
    return ds
