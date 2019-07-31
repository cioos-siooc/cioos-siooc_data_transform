# IOS Data Conversion

This python package was created to aid in the conversion of data from an IOS (Institute of Ocean Sciences) specific format to netCDF files.
The netCDF files are compatible with the CF conventions and ERDDAP requirements. 


## Description of tools
* Ocean data class: This contains the methods (functions) to read different sections from a data file in the IOS format. Data is stored in state variables that describe the class.
* netCDF4 file class: This has all the methods required to write a netCDF file in the standard format. Data is held in object untill write_file function is executed.
* netCDF4 variable class: Hold variable data and definition. List of these objects are passed to netCDF file class to write out in a standard format.

## Getting Started / Installing

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

* Install a working Python 3X distribution (see below for more details). Or create a new env (recommended)
* Clone this github repo to a local folder
* Open a terminal and change into folder with README.md
* Run "$ python setup.py install" to install the package. Dependencies should be automatically installed as required.

### Prerequisites

* Linux (or Unix-like) environment
* Python 3X environment. See (https://docs.conda.io/en/latest/miniconda.html) for recommended installation instructions
* Optional: Create a new python environment ($ conda create -n my-environment-name)

## Examples

See example.py for sample on how to use the package to convert IOS CTD datafiles to netCDF format.

## Authors

* **Pramod Thupaki** - pramod.thupaki@hakai.org

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is free and open to use by public - see the [LICENSE](LICENSE) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc

