import pandas as pd
import numpy as np
import warnings
import datetime as dt
import re
import json
import os

"""
This Module regroup diverse methods used to handle xarray datasets and generate CIOOS/ERDDAP compliant datasets.
"""


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
        if ds[var].dtype not in [float, int, "float64", "float32", "int64", "int32"]:
            # Convert Datetime to seconds since 1970-01-01
            if ds[var].dtype.name.startswith("datetime"):
                # Convert Datetime to seconds since 1970-01-01
                if "units" in ds[var].attrs:
                    ds[var].attrs.pop("units")
                # Timezone aware data
                if "tz" in ds[var].dtype.name:
                    timezone = "Z"
                else:
                    timezone = ""

                # Format encoding output
                ds[var].encoding["units"] = (
                    "seconds since 1970-01-01 00:00:00" + timezone
                )

            else:
                # Should be a string
                ds[var] = ds[var].astype(str)

    return ds


def standardize_variable_attributes(ds):
    """
    Method to generate simple generic variable attributes and reorder attributes in a consistent order.
    """
    attribute_order = [
        "long_name",
        "units",
        "time_zone",
        "scale",
        "standard_name",
        "sdn_parameter_name",
        "sdn_parameter_urn",
        "sdn_uom_urn",
        "sdn_uom_name",
        "ioos_category",
        "gf3_code",
        "source",
        "reference",
        "comments",
        "definition",
        "ancillary_variables",
        "cell_method",
        "actual_range",
        "valid_range",
        "value_min",
        "value_max",
        "mising_value",
        "_FillValue",
        "fileAccessBaseUrl",
        "_Encoding",
        "grid_mapping",
    ]
    for var in ds:
        if (
            ds[var].dtype in [float, int, "float32", "float64", "int64", "int32"]
            and "flag_values" not in ds[var].attrs
        ):
            ds[var].attrs["actual_range"] = (
                ds[var].min().item(0),
                ds[var].max().item(0),
            )

        # Sort attributes by order provided
        sorted_attributes = {
            key: ds[var].attrs[key] for key in attribute_order if key in ds[var].attrs
        }

        # If any left over add the rest
        sorted_attributes.update(ds[var].attrs)

        # Drop empty attributes
        empty_att = [key for key, att in sorted_attributes.items() if att is None]
        for key in empty_att:
            sorted_attributes.pop(key)

        ds[var].attrs = sorted_attributes
    return ds


def get_spatial_coverage_attributes(
    ds, time="time", lat="latitude", lon="longitude", depth="depth",
):
    """
    This method generates the geospatial and time coverage attributes associated to an xarray dataset.
    """
    # TODO add resolution attributes
    time_spatial_coverage = {}
    # time
    if time in ds:
        time_spatial_coverage.update(
            {
                "time_coverage_start": str(ds[time].min().values),
                "time_coverage_end": str(ds[time].max().values),
                "time_coverage_duration": pd.to_timedelta(
                    (ds[time].max() - ds[time].min()).values
                ).isoformat(),
            }
        )

    # lat/long
    if lat in ds and lon in ds:
        time_spatial_coverage.update(
            {
                "geospatial_lat_min": ds[lat].min().values,
                "geospatial_lat_max": ds[lat].max().values,
                "geospatial_lat_units": ds[lat].attrs.get("units"),
                "geospatial_lon_min": ds[lon].min().values,
                "geospatial_lon_max": ds[lon].max().values,
                "geospatial_lon_units": ds[lon].attrs.get("units"),
            }
        )

    # depth coverage
    if depth in ds:
        time_spatial_coverage.update(
            {
                "geospatial_vertical_min": ds[depth].min().values,
                "geospatial_vertical_max": ds[depth].max().values,
                "geospatial_vertical_units": ds[depth].attrs["units"],
                "geospatial_vertical_positive": "down",
            }
        )

    # Add to global attributes
    ds.attrs.update(time_spatial_coverage)
    return ds
