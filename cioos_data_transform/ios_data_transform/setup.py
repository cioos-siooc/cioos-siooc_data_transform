import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ios_data_transform",
    version="0.0.1",
    author="Pramod Thupaki",
    author_email="pramod.thupaki@hakai.org",
    description="toolbox with utilities to read data in IOS format and convert to netCDF format",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(),
    classifiers=["Programming Language :: Python :: 2",
        "Operating System :: OS Independent"],
)
