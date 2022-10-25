import logging

import numpy as np
import xarray as xr

logger = logging.getLogger(__name__)

standard_name_range = {
    "sea_water_temperature": [-20, 100],
    "sea_water_salinity": [-5, 100],
}


def review_data_range(dataset: xr.Dataset, drop_bad_values: bool = True) -> xr.Dataset:
    """Review each standard_name variables and drop any values out of the acceptable range.s"""
    for standard_name, (min_range,max_range) in standard_name_range.items():
        for var in dataset.filter_by_attrs(standard_name=standard_name):
            in_range = (min_range < dataset[var]) & (dataset[var] < max_range)
            if any(~in_range):
                logger.warning(
                    "Some values in %s is out of range %s %s.",
                    var,
                    standard_name_range,
                    (min_range,max_range),
                )
                if drop_bad_values:
                    dataset[var] = xr.where(in_range, dataset[var], np.nan)
    return dataset
