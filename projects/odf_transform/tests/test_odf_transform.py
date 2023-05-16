"""This module apply different tests to the ODF conversion package."""
import logging
import os
import re
import unittest
from glob import glob

import odf_transform.process
import xarray as xr
import pytest

logger = logging.getLogger(__name__)

mli_test_odfs = glob(
            "./projects/odf_transform/sample_data/mli_samples/**/*.ODF", recursive=True
        )
mli_config = {
    **odf_transform.process.read_config(
        "./projects/odf_transform/config.json"
    ),
    "organisationVocabulary": ["MLI", "GF3"],
    "addFileNameSuffix": "_test",
    "output_path": None
}

bio_test_odfs = glob(
            "./projects/odf_transform/sample_data/bio_samples/**/*.ODF", recursive=True
        )
bio_config = {
    **odf_transform.process.read_config(
        "./projects/odf_transform/config.json"
    ),
    "organisationVocabulary": ["BIO", "GF3"],
    "addFileNameSuffix": "_test",
    "output_path": None
}

@pytest.mark.parametrize(
    "file",
    bio_test_odfs
)
def test_bio_odf_sample_data_conversion(file):
    """Test DFO BIO ODF conversion to xarray dataset"""
    odf_transform.process.odf_to_netcdf(file, config=bio_config)

@pytest.mark.parametrize(
    "file",
    mli_test_odfs
)
def test_mli_odf_sample_data_conversion(file):
    """Test DFO MLI ODF conversion to xarray dataset"""
    
    odf_transform.process.odf_to_netcdf(file, config=mli_config)

@pytest.mark.parametrize(
    "file",
    bio_test_odfs
)
def test_bio_odf_converted_netcdf_vs_references(file):
    """Test DFO BIO ODF conversion to NetCDF vs reference files"""

    def ignore_from_attr(attr, expression, placeholder):
        """Replace expression in both reference and test files which are expected to be different."""
        ref.attrs[attr] = re.sub(expression, placeholder, ref.attrs[attr])
        test.attrs[attr] = re.sub(expression, placeholder, test.attrs[attr])

    # Run bio odf conversion
    test_bio_odf_sample_data_conversion(file)
    nc_file = file + '_reference.nc'
    ref = xr.open_dataset(nc_file)
    nc_file_test = nc_file.replace("_reference.nc", "_test.nc")
    if not os.path.isfile(nc_file_test):
        raise RuntimeError(f"{nc_file_test} was not generated.")
    test = xr.open_dataset(nc_file_test)

    # Add placeholders to specific fields in attributes
    ignore_from_attr(
        "history",
        r"cioos_data_trasform.odf_transform V \d+\.\d+.\d+",
        "cioos_data_trasform.odf_transform V VERSION",
    )
    ignore_from_attr(
        "history", r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ", "TIMESTAMP"
    )
    ref.attrs["date_created"] = "TIMESTAMP"
    test.attrs["date_created"] = "TIMESTAMP"

    # If file are identical good
    if ref.identical(test):
        return

    # find through netcdf files differences
    for key, value in ref.attrs.items():
        if test.attrs.get(key) != value:
            logger.error(
                "Global attribute ds.attrs[%s] is different from reference file",
                key,
            )

    if test.attrs.keys() != ref.attrs.keys():
        logger.error("A new global attribute %s was detected.", set(test.attrs.keys()) - set(ref.attrs.keys()))

    ref_variables = list(ref) + list(ref.coords)
    test_variables = list(test) + list(test.coords)
    if ref_variables.sort() != test_variables.sort():
        logger.error('A variable mismatch exist between the reference and test files')

    for var in ref_variables:
        # compare variable
        if not ref[var].identical(test[var]):
            logger.error(
                "Variable ds[%s] is different from reference file", var
            )
        if (ref[var].values != test[var].values).any():
            logger.error(
                "Variable ds[%s].values are different from reference file", var
            )

        # compare variable attributes
        for attr, value in ref[var].attrs.items():
            is_not_same_attr = test[var].attrs.get(attr) != value
            if isinstance(is_not_same_attr, bool) and not is_not_same_attr:
                continue
            elif isinstance(is_not_same_attr, bool) and is_not_same_attr:
                logger.error(
                    "Variable ds[%s].attrs[%s] list is different from reference file",
                    var,
                    attr,
                )
            elif (is_not_same_attr).any():
                logger.error(
                    "Variable ds[%s].attrs[%s] is different from reference file",
                    var,
                    attr,
                    )
    raise RuntimeError(
        f"Converted file {nc_file_test} is different than the reference: {nc_file}"
    )
