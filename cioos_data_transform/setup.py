import setuptools

with open("./README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cioos_data_transform",
    version="1.0.0",
    author="Pramod Thupaki",
    author_email="pramod.thupaki@hakai.org",
    description="Toolbox to read data in IOS format and convert to netCDF format",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cioos-siooc/cioos-siooc_data_transform",
    packages=setuptools.find_packages(),
    install_requires=["numpy", "fortranformat", "netCDF4", "pytz", "shapely", "gsw==3.3.1", "pandas"],
    classifiers=["Programming Language :: Python :: 3", "Operating System :: OS Independent"],
)
