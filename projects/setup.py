import setuptools

setuptools.setup(
    name="cioos_data_transform projects",
    version="1.0.0",
    description="Data conversion projects that use cioos_data_transform",
    packages=setuptools.find_packages(),
    install_requires=[
        "numpy",
        "xarray",
        "requests",
        "pandas",
        "rpy2",
        "xmltodict",
        "tqdm",
        "pytz",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
