# IOS Data Conversion

This python package was created to aid in the conversion of data from an IOS (Institute of Ocean Sciences) specific format to netCDF files.
The netCDF files are compatible with the CF conventions and ERDDAP requirements. 


## Description of classes and important methods
* ObsFile (extensions: CtdFile, BotFile, etc.): Includes variables and methods to read data in the IOS format. Information is processed and stored in state variables used by OceanNcFile (or similar).
* OceanNcFile (extensions: CtdNcFile etc.): Includes methods required to write a netCDF file in the standard format. Data is held in object untill write_file function is executed.
* OceanNcVar: Holds variable data and definition. List of these objects are passed to OceanNcFile to write out in a standard format.

## Getting Started / Installing

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

* Install a working Python 3X distribution (see below for more details). Or create a new env (recommended)
* Clone this github repo to a local folder
* Open a terminal and change into folder with README.md
* cd into GSW-Python folder and install toolbox using '$ pip install .'
* Run "$ python setup.py install" to install the package. Dependencies should be automatically installed as required.

### Prerequisites

* Linux (or Unix-like) environment
* Python >= 3.5 environment. See (https://docs.conda.io/en/latest/miniconda.html) for recommended installation instructions
* Optional: Create a new python environment ($ conda create -n my-environment-name) 
* Recommended: Use conda-forge channel (https://conda-forge.org/) for packages where available

## Examples

See ios_data_transform/samples/example.py for sample on how to use the package to convert IOS CTD datafiles to netCDF format.
Script used to injest IOS data into CIOOS are also in ./ios_data_transform/samples/

Codes used to test the data conversion are in ./ios_data_transform/tests/

## Authors

* **Pramod Thupaki** - pramod.thupaki@hakai.org

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is free and open to use by public - see the [LICENSE](LICENSE) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc

