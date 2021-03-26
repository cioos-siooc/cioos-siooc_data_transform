# cioos-siooc_data_transform

## Getting started with ODF to NetCDF conversion development

1. Setup your Python3 virtual environment

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

   If you plan to run the R oce package using rpy2, use Python 3.7 or below
   (rpy2 requirement), and use the --system-site-packages argument (for use
   of system R) in the python3 command above.

1. Install cioos_data_transform package

   ```sh
   cd cioos_data_transform/cioos_data_transform
   pip install -e .
   ```

1. Convert the sample ODF files to NetCDF, creating a folder 'temp' for output

   ```sh
   cd odf_transform/test
   mkdir temp
   python test_odf.py
   ```

1. Now temp will be full of NetCDF files

   ```sh
   ls temp
   ```
