import numpy as np


def convert_variables_to_erddap_format(ds):
    for var in ds:
        # Convert Datetime to seconds since 1970-01-01
        if ds[var].dtype not in [float, int]:

            if ds[var].dtype in ['datetime64[ns]']:
                # Convert Datetime to seconds since 1970-01-01
                if 'units' in ds[var].attrs:
                    ds[var].attrs.pop('units')
                ds[var].encoding['units'] = 'seconds since 1970-01-01T00:00:00'
            # elif ds[var].dtype in ['datetime64tz[ns]']:
            #     # Convert Timezone aware Datetime to seconds since 1970-01-01T00:00:00Z
            #     ds[var].encoding['units'] = 'seconds since 1970-01-01T00:00:00Z'
            #     ds[var].attrs['timezone'] = 'UTC'
            else:
                # Should be a string
                ds[var] = ds[var].astype("|S")

            print(var)

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
