#!/usr/bin/env python3

"""
Use OCE R package from Python to read an ODF file
You will need to install R and libraries 'oce' and 'RJSONIO':
> library(devtools)
> install_github("dankelley/oce", ref="develop")
> install.packages("RJSONIO", dependencies = TRUE)
> library(RJSONIO)

Install Python pacakges:
pip install pandas xarray rpy2

"""

import glob
import json

from rpy2.robjects.packages import importr
from rpy2.rinterface_lib.embedded import RRuntimeError
 
oce = importr("oce")
rjsonio = importr("RJSONIO")

ODF_DIRECTORY = "test_files"


def read_odf_py(filename):
    """
    interfaces with the the OCE R library by serializing the oce
    object to/from json
    """

    odf = oce.read_odf(filename)

    json_odf = rjsonio.toJSON(odf)

    json_odf_unescaped = (
        str(json_odf)
        .encode("windows-1252")
        .decode("unicode_escape")
        .replace('[1] "', "")[0:-2]
    )
    odf = json.loads(json_odf_unescaped)
    return odf


for odf_path in glob.glob(ODF_DIRECTORY + "/**/*", recursive=True):
    if odf_path.upper().endswith(".ODF"):
        print(odf_path)
        try:
            odf_dict = read_odf_py(odf_path)
            f = open(f"{odf_path}.json", "w")
            f.write(json.dumps(odf_dict, indent=4, ensure_ascii=False))
        except RRuntimeError as e:
            print(e)
