# IOS Data Conversion

This python package was created to aid in the conversion of data from an IOS (Institute of Ocean Sciences) specific format to netCDF files.
The netCDF files are compatible with the CF conventions and ERDDAP requirements.

## Description of classes and important methods

- ObsFile (extensions: CtdFile, BotFile, etc.): Includes variables and methods to read data in the IOS format. Information is processed and stored in state variables used by OceanNcFile (or similar).
- OceanNcFile (extensions: CtdNcFile etc.): Includes methods required to write a netCDF file in the standard format. Data is held in object until write_file function is executed.
- OceanNcVar: Holds variable data and definition. List of these objects are passed to OceanNcFile to write out in a standard format.

## Getting Started / Installing

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

- Install a working Python 3X distribution (see **Prerequisites** for more details)
  - Recommended: Create a new env using command 'conda create -n myenv python=3.X'
- Clone this github repo to a local folder
- Open a terminal and change into folder with README.md

- Run command 'pip install -e .' to install the ios_data_transform and dependencies automatically
  - Note: -e flag installs package in 'editable' mode

### Prerequisites

- Linux (or Unix-like) environment
  - Currently in testing for Windows environment (March 02, 2020)
- Python >= 3.5 environment. See (https://docs.conda.io/en/latest/miniconda.html) for recommended installation instructions
- Recommended: Use conda-forge channel (https://conda-forge.org/) for packages where available

## Examples

See ios_data_transform/tests/test.py for example on how to use the package to read and convert IOS datafiles

## Authors

- **Pramod Thupaki** - pramod.thupaki@hakai.org
- **Hana Hourston**
- **Jessy Barrette**

See also the list of [contributors](https://github.com/your/project/contributors) who participated in this project.

## License

This project is free and open to use by public - see the [LICENSE](LICENSE) file for details

## Acknowledgments

## References

https://teos-10.github.io/GSW-Python/index.html
