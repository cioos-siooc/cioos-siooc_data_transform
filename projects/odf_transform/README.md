# ODF Transform

This project converts ODF files to NetCDF

## Installation

1. Create a new conda `cioos_transform` environment running Python 3.7 (required for Windows):
``` shell
conda create --name cioos_transform python=3.7
```
2. Activate the environement:
```shell
conda activate cioos_transform
```
3. From the root of this repo run:

```shell
pip install -e cioos_data_transform
pip install -e projects
```

## Running

Specifying input (ODF) and ouput (NetCDF) folders in the default config file

```shell
python odf_transform
```

Specifying a config file using `-c`
```shell
python odf_transform -c /path/to/config/config.json
```

Specifying input and output file/directories using `-i` and `-o`

```shell
python odf_transform -i ../sample_data/test_files/ -o ./netcdf_files
```

Running on a single file:

```shell
python odf_transform ../sample_data/test_files/CTD_1994038_147_1_DN.ODF
```
