"""This module apply different tests to the ODF conversion package."""
import logging
import os
import re
import unittest
from glob import glob

import odf_transform.process
import xarray as xr

logger = logging.getLogger(__name__)


class TestODFConversion(unittest.TestCase):
    """Test DFO ODF File conversion."""

    def test_bio_odf_sample_data_conversion(self):
        """Test DFO BIO ODF conversion to xarray dataset"""
        default_odf_config = odf_transform.process.read_config(
            "./projects/odf_transform/config.json"
        )
        sample_files = glob(
            "./projects/odf_transform/sample_data/bio_samples/**/*.ODF", recursive=True
        )

        # Make sure to define test generated files
        default_odf_config["organisationVocabulary"] = ["BIO", "GF3"]
        default_odf_config["addFileNameSuffix"] = "_test"
        default_odf_config["output_path"] = None

        for file in sample_files:
            odf_transform.process.odf_to_netcdf(file, config=default_odf_config)

    def test_mli_odf_sample_data_conversion(self):
        """Test DFO MLI ODF conversion to xarray dataset"""
        default_odf_config = odf_transform.process.read_config(
            "./projects/odf_transform/config.json"
        )
        sample_files = glob(
            "./projects/odf_transform/sample_data/mli_samples/**/*.ODF", recursive=True
        )

        # Make sure to define test generated files
        default_odf_config["organisationVocabulary"] = ["GF3"]
        default_odf_config["addFileNameSuffix"] = "_test"
        default_odf_config["output_path"] = None

        for file in sample_files:
            odf_transform.process.odf_to_netcdf(file, config=default_odf_config)

    def test_bio_odf_converted_netcdf_vs_references(self):
        """Test DFO BIO ODF conversion to NetCDF vs reference files"""

        def ignore_from_attr(attr, expression, placeholder):
            """Replace expression in both reference and test files which are expected to be different."""
            ref.attrs[attr] = re.sub(expression, placeholder, ref.attrs[attr])
            test.attrs[attr] = re.sub(expression, placeholder, test.attrs[attr])

        # Run bio odf conversion
        self.test_bio_odf_sample_data_conversion()

        # Compare to reference files
        nc_files = glob(
            "./projects/odf_transform/sample_data/bio_samples/**/*.ODF_reference.nc",
            recursive=True,
        )
        for nc_file in nc_files:
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

            if not ref.identical(test):
                for key, value in ref.attrs.items():
                    if test.attrs[key] != value:
                        logger.error(
                            "Global attribute ds.attrs[%s] is different from reference file",
                            key,
                        )
                for var in ref:
                    if not ref[var].identical(test[var]):
                        logger.error(
                            "Variable ds[%s] is different from reference file", var
                        )
                raise RuntimeError(
                    f"Converted file {nc_file_test} is different than the reference: {nc_file}"
                )
