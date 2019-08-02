import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ios_data_transform",
    version="0.0.1",
    author="Pramod Thupaki",
    author_email="pramod.thupaki@hakai.org",
    description="Toolbox to read data in IOS format and convert to netCDF format",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(),
    install_requires=['numpy', 'fortranformat', 'netCDF4', 'pytz'],
    classifiers=["Programming Language :: Python :: 3",
        "Operating System :: OS Independent"],
)
