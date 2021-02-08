#!/usr/bin/env python3

"""
Use OCE R package from Python to read an ODF file

You will need to install R and libraries 'oce' and 'RJSONIO' by running
the following commands in R:

> library(devtools)
> install_github("dankelley/oce", ref="develop")
> install.packages("oce", dependencies = TRUE)
> install.packages("RJSONIO", dependencies = TRUE)
> library(RJSONIO)

"""

import json
import pandas as pd
from rpy2.robjects.packages import importr
import glob

oce = importr('oce')
rjsonio = importr('RJSONIO')


def read_odf_py(filename):
    """
    interfaces with the the OCE R library by serializing the oce
    object to/from json
    """

    odf = oce.read_odf(filename)

    json_odf = rjsonio.toJSON(odf)
    json_odf_unescaped = str(json_odf).encode('utf-8').decode('unicode_escape').replace('[1] "', '')[0:-2]
    odf = json.loads(json_odf_unescaped)
    return odf


flist = glob.glob('./test_files/*.ODF')
for f in flist:
    odf_dict = read_odf_py(f)

    # load data into Pandas dataframe
    df = pd.DataFrame.from_dict(odf_dict['data'])

    # metadata is a nested dictionary
    metadata = odf_dict['metadata']

    print(f, metadata.keys())
