#!/usr/bin/env python3

"""

Convert all ODF files in a directory (recursive) to JSON

See README for installation instructions

Usage:
python odf_to_json.py test_files

"""

import argparse
import glob
import json

from rpy2.rinterface_lib.embedded import RRuntimeError
from rpy2.robjects.packages import importr

oce = importr("oce")
rjsonio = importr("RJSONIO")


parser = argparse.ArgumentParser(description="Convert ODF files to JSON.")
parser.add_argument("odf_directory", help="Directory to recursively scan for ODF files")

args = parser.parse_args()
ODF_DIRECTORY = args.odf_directory


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


def convert_sample_files():
    """
    Convert sample files in ODF_DIRECTORY recursively
    """
    for odf_path in glob.glob(ODF_DIRECTORY + "/**", recursive=True):
        if odf_path.upper().endswith(".ODF"):
            print(odf_path)
            try:
                odf_dict = read_odf_py(odf_path)

                # processingLog records the time and local directory, so
                # the resulting JSON will be different every run. Deleting this
                # to avoid new changes in git
                del odf_dict["processingLog"]

                f = open(f"{odf_path}.json", "w")

                # clean up the "' and '" that appear
                json_str = (
                    json.dumps(odf_dict, indent=4, ensure_ascii=False)
                    .replace("\"'", '"')
                    .replace("'\"", '"')
                )

                # test that the JSON is still parseable, otherwise error out
                json.loads(json_str)

                f.write(json_str)
                f.close()

            except RRuntimeError as e:
                print(e)


convert_sample_files()
