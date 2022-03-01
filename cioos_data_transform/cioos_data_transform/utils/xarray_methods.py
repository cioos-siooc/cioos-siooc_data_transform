import pandas as pd
import numpy as np
import datetime as dt


"""
This Module regroup diverse methods used to handle xarray datasets and generate CIOOS/ERDDAP compliant datasets.
"""


def history_input(comment, date=dt.datetime.now()):
    if type(date) == dt.datetime:
        return f"{date.strftime('%Y-%m-%dT%H:%M:%SZ')} {comment}\n"
    else:
        return f"{date} {comment}\n"


def standardize_dataset(ds):
    """Global method that applies the different standardizations tools."""
    ds = get_spatial_coverage_attributes(ds)
    ds = convert_variables_to_erddap_format(ds)
    ds = standardize_variable_attributes(ds)
    ds.attrs = standardize_global_attributes(ds.attrs)
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
        if ds[var].dtype not in [float, int, "float64", "float32", "int64", "int32"]:
            # Convert Datetime to seconds since 1970-01-01
            if ds[var].dtype.name.startswith("datetime"):
                # Convert Datetime to seconds since 1970-01-01
                if "units" in ds[var].attrs:
                    ds[var].attrs.pop("units")
                # Timezone aware data
                if "tz" in ds[var].dtype.name:
                    timezone = "Z"
                    ds[var].attrs["timezone"] = "UTC"
                else:
                    timezone = ""

                # Format encoding output
                ds[var].encoding = {
                    "units": "seconds since 1970-01-01 00:00:00" + timezone,
                    "dtype": "float64",
                }

            else:
                # Should be a string
                ds[var] = ds[var].astype(str)

    return ds


def standardize_attributes_values(attrs, order):
    new_attrs = {}
    # Reorder variables
    ordered_attrs = {attr: attrs.pop(attr) for attr in order if attr in attrs}
    ordered_attrs.update(attrs)

    # Apply conversion and ignore empty attributes
    for attr, value in ordered_attrs.items():
        if value in [None, "", pd.NaT]:
            continue
        if type(value) == pd.Timestamp:
            if value.tz:
                value = value.astimezone("UTC")
            value = value.strftime("%Y-%m-%dT%H:%M:%SZ")

        new_attrs[attr] = value

    return new_attrs


def standardize_global_attributes(attrs):
    attribute_order = [
        "organization",
        "institution",
        "institution_fr",
        "country",
        "ioc_country_code",
        "iso_3166_country_code",
        "ices_edmo_code",
        "sdn_institution_urn",
        "program",
        "project",
        "infoUrl",
        "title",
        "summary",
        "processing_level",
        "platform",
        "sdn_platform_urn",
        "platform_imo_number",
        "platform_vocabulary",
        "cruise_name",
        "cruise_number",
        "cruise_description",
        "chief_scientist",
        "mission_start_date",
        "mission_end_date",
        "platform",
        "id",
        "naming_authority",
        "original_filename",
        "event_number",
        "profile_direction",
        "event_start_time",
        "event_end_time",
        "initial_latitude",
        "initial_longitude",
        "station",
        "instrument",
        "instrument_type",
        "instrument_model",
        "instrument_serial_number",
        "instrument_vocabulary",
        "instrument_description",
        "date_created",
        "creator_name",
        "creator_email",
        "creator_country",
        "creator_sector",
        "creator_url",
        "creator_type",
        "publisher_name",
        "publisher_email",
        "publisher_country",
        "publisher_url",
        "publisher_type",
        "publisher_institution",
        "date_modified",
        "history",
        "time_coverage_start",
        "time_coverage_end",
        "time_coverage_duration",
        "time_coverage_resolution",
        "geospatial_lat_min",
        "geospatial_lat_max",
        "geospatial_lat_units",
        "geospatial_lon_min",
        "geospatial_lon_max",
        "geospatial_lon_units",
        "geospatial_vertical_min",
        "geospatial_vertical_max",
        "geospatial_vertical_units",
        "geospatial_vertical_positive",
        "geospatial_vertical_resolution",
        "cdm_data_type",
        "cdm_profile_variables",
        "keywords",
        "acknowledgement",
        "license",
        "keywords_vocabulary",
        "standard_name_vocabulary",
        "Conventions",
    ]
    return standardize_attributes_values(attrs, attribute_order)


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
            ds[var].attrs["actual_range"] = tuple(
                np.array((ds[var].min().item(0), ds[var].max().item(0))).astype(
                    ds[var].dtype
                )
            )

        ds[var].attrs = standardize_attributes_values(ds[var].attrs, attribute_order)
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
        ds["depth"].attrs["positive"] = ds["depth"].attrs.get("positive", "down")
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
