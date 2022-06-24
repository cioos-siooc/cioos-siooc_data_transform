import os
import re
import unittest
from glob import glob

import odf_transform.process
import xarray as xr


class TestBIOODFConversion(unittest.TestCase):
    def test_bio_odf_sample_data_conversion(self):

        default_odf_config = odf_transform.process.read_config(
            "./projects/odf_transform/config.json"
        )
        sample_files = glob(
            "./projects/odf_transform/sample_data/bio_samples/**/*.ODF", recursive=True
        )

        # Make sure to define test generated files
        default_odf_config["organisationVocabulary"] = ["BIO", "GF3"]
        default_odf_config["addFileNameSuffix"] = "_test"

        for file in sample_files:
            odf_transform.process.convert_odf_file(file, config=default_odf_config)

    def test_mli_odf_sample_data_conversion(self):

        default_odf_config = odf_transform.process.read_config(
            "./projects/odf_transform/config.json"
        )
        sample_files = glob(
            "./projects/odf_transform/sample_data/mli_samples/**/*.ODF", recursive=True
        )

        # Make sure to define test generated files
        default_odf_config["organisationVocabulary"] = ["GF3"]
        default_odf_config["addFileNameSuffix"] = "_test"

        for file in sample_files:
            odf_transform.process.convert_odf_file(file, config=default_odf_config)

    def test_bio_odf_converted_netcdf_vs_references(self):
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

            # Replace history timestamps by a placeholder
            timestamp_format = r"\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ"
            ref.attrs["history"] = re.sub(
                timestamp_format, "TIMESTAMP", ref.attrs["history"]
            )
            test.attrs["history"] = re.sub(
                timestamp_format, "TIMESTAMP", test.attrs["history"]
            )

            if not ref.identical(test):
                raise RuntimeError(
                    f"Converted file {nc_file_test} is different than the reference: {nc_file}"
                )
