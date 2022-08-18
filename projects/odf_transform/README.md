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

## How to

Specifying input (ODF) and ouput (NetCDF) folders in the default config file

```shell
python odf_transform
```

Specifying a config file using `-c` (recommanded)
```shell
python odf_transform -c /path/to/config/config.json
```

## Configuration
Here's a description of the configuration to be used while running the ODF conversion. The variables {ODF_TRANSFORM_MODULE_PATH} can be use accross the configuration to define the path where the actual package is installed.

```json
{
    "fileDir": Input path compatible with glob to retrieve the different ODF files. 
                default: "{ODF_TRANSFORM_MODULE_PATH}/sample_data/**/*.ODF",
    "recursive": Looking in subdirectories ([true]/false),
    "pathRegex": Filter subdirectories by considering only expressions (default null),
    "program_log_path": Folder where csv files associated to each programs are present. Program Logs list the different mission associated to a program and their associated global attributes to add to each individual files null,
    "output_path": Path where to output converted files. If null, the data is saved next to the original INT (default: "./output"),
    "subfolder_attribute_output_path": {
        Subfolder structure to respect while saving the converted netcdf to their output_path. Each key of that dictory correspond to the global attribute to look for, and the value correspond to the default value. If null(None), no subdirectory is generated.
        "program": "Others",
        "project": null,
        "cruise_name": null
    },
    "addFileNameSuffix": Suffix to add to each converted files. "_dev",
    "overwrite": Overwrite already existing NetCDFs (true/[false],
    "geographic_area_reference_files": [
        List of geojson geographic areas to use
        "{ODF_TRANSFORM_MODULE_PATH}/geojson_files/ios_polygons.geojson",
        "{ODF_TRANSFORM_MODULE_PATH}/geojson_files/MLI_St-LawrenceBoxes.geojson",
        "{ODF_TRANSFORM_MODULE_PATH}/geojson_files/csasAtlPhys_climatePolygons.geojson"
    ],
    "reference_stations_reference_files": [
        list of geojson files listing the different stations to associate data to
        "{ODF_TRANSFORM_MODULE_PATH}/reference_stations.geojson"
    ],
    "maximum_distance_from_station_km": maximum acceptable distance to match the nearest station available in the reference stations to the data  3,
    "vocabularyFile": vocabulary file used to match the different ODF GF3 terms to their BODC and CF equivalement (
                        "{ODF_TRANSFORM_MODULE_PATH}/reference_vocabulary.csv",
    "reference_platforms_files": list of L06 Platforms [
        "{ODF_TRANSFORM_MODULE_PATH}/reference_platforms.csv"
    ],
    "attribute_mapping_corrections_files": attribute mapping correction list[
        "{ODF_TRANSFORM_MODULE_PATH}/attribute_corrections.json"
    ],
    "file_specific_attributes_path": file specific attribute corrections null,
    "organisationVocabulary": vocabulary organizations to consider and their other of preference[
        "BIO",
        "GF3"
    ],
    "global_attributes": { global attributes to add to all the ODF converted},
    "flag_convention": {
        # Default Flag convention used in the ODFs
        "default": {
            "dtype": "int32",
            "standard_name": "status_flag",
            "coverage_content_type": "qualityInformation",
            "ioos_category": "Quality",
            "flag_values": [
                0,
                1,
                2,
                3,
                4,
                5,
                9
            ],
            "flag_meanings": "not_evaluated correct inconsistent_with_other_values doubtful erroneous modified missing"
        },
        # Any more flag variable specific convention can be defined below.
        "QCFF_01": {
            "dtype": "int32",
            "standard_name": "status_flag",
            "coverage_content_type": "qualityInformation",
            "ioos_category": "Quality",
            "flag_values": [
                0,
                1
            ],
            "flag_meanings": "undefined undefined"
        },
        "FFFF_01": {
            "dtype": "int32",
            "standard_name": "status_flag",
            "coverage_content_type": "qualityInformation",
            "ioos_category": "Quality",
            "flag_values": [
                0,
                1
            ],
            "flag_meanings": "undefined undefined"
        }
    }
}
```