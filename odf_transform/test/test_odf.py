import sys
import os
import numpy as np
import json

sys.path.insert(0, "../../")

from odf_transform.odfCls import CtdNcFile, NcVar
from odf_transform.utils.utils import get_geo_code, read_geojson
from ios_data_transform import is_in
from datetime import datetime
from pytz import timezone
import glob


def write_ctd_ncfile(outfile, odf_data, **kwargs):
    """
    use data and methods in ctdcls object to write the CTD data into a netcdf file
    author:
    inputs:
        outfile: output file name to be created in netcdf format
        odf_data: dict with data from odf file converted to json using oce package
        **kwargs: optional arguments
    output:
        NONE
    """
    # print(kwargs.keys())
    out = CtdNcFile()
    # write global attributes
    out.featureType = "profile"
    try:
        out.summary = kwargs["summary"]
        out.title = kwargs["title"]
        out.institution = data["metadata"]["institute"]
        out.infoUrl = kwargs["infoUrl"]
        out.description = kwargs["description"]
        out.keywords = kwargs["keywords"]
        out.acknowledgement = kwargs["acknowledgement"]
        out.naming_authority = "COARDS"
        out.creator_name = kwargs["creator_name"]
        out.creator_email = kwargs["creator_email"]
        out.creator_url = kwargs["creator_url"]
        out.license = kwargs["license"]
        out.project = data["metadata"]["cruise"]
        out.keywords_vocabulary = kwargs["keywords_vocabulary"]
        out.Conventions = kwargs["Conventions"]
    except KeyError as e:
        raise Exception(
            f"Unable to find following value for {e} in the config file..."
        )
    out.cdm_profile_variables = "time"
    # write full original header, as json dictionary
    out.header = json.dumps(
        data["metadata"]["header"], ensure_ascii=False, indent=False
    )
    # initcreate dimension variable
    # use length of first variable to define length of profile
    out.nrec = len(data["data"][list(data["data"].keys())[0]])
    # add variable profile_id
    ncfile_var_list = []
    ncfile_var_list.append(
        NcVar(
            "str_id",
            "filename",
            None,
            data["metadata"]["filename"].split("/")[-1],
        )
    )
    # add administration variables
    ncfile_var_list.append(NcVar("str_id", "country", None, "Canada"))
    ncfile_var_list.append(
        NcVar("str_id", "cruise_id", None, data["metadata"]["cruiseNumber"])
    )
    ncfile_var_list.append(
        NcVar("str_id", "scientist", None, data["metadata"]["scientist"])
    )
    ncfile_var_list.append(
        NcVar("str_id", "platform", None, data["metadata"]["ship"])
    )
    ncfile_var_list.append(
        NcVar(
            "str_id",
            "instrument_type",
            None,
            data["metadata"]["type"] + " " + data["metadata"]["model"],
        )
    )
    ncfile_var_list.append(
        NcVar(
            "str_id",
            "instrument_serial_number",
            None,
            data["metadata"]["serialNumber"],
        )
    )
    # add locations variables
    ncfile_var_list.append(
        NcVar("lat", "latitude", "degrees_north", data["metadata"]["latitude"])
    )
    ncfile_var_list.append(
        NcVar(
            "lon", "longitude", "degrees_east", data["metadata"]["longitude"]
        )
    )
    ncfile_var_list.append(
        NcVar(
            "str_id",
            "geographic_area",
            None,
            get_geo_code(
                [
                    float(data["metadata"]["longitude"]),
                    float(data["metadata"]["latitude"]),
                ],
                kwargs["polygons_dict"],
            ),
        )
    )
    event_id = "{}-{}".format(
        data["metadata"]["eventQualifier"], data["metadata"]["eventNumber"]
    )
    ncfile_var_list.append(NcVar("str_id", "event_number", None, event_id))
    # create unique ID for each profile
    profile_id = "{}-{}-{}".format(
        data["metadata"]["cruiseNumber"],
        data["metadata"]["eventNumber"],
        data["metadata"]["eventQualifier"],
    )
    print("Profile ID:", profile_id)
    out.id = profile_id
    ncfile_var_list.append(NcVar("profile", "profile", None, profile_id))
    # pramod - someone should check this...
    date_obj = datetime.utcfromtimestamp(data["metadata"]["startTime"])
    date_obj = date_obj.astimezone(timezone("UTC"))
    ncfile_var_list.append(NcVar("time", "time", None, [date_obj]))

    for i, var in enumerate(data["data"].keys()):
        #
        # ***********  TODO: CREATE A FUNCTION TO CONVERT UNITS FROM DICTIONARY FORMAT TO PLAIN STRING   ************
        # ***********  TODO: DETERMINE BODC/GF3 CODE FROM THE UNITS AND VARIABLE NAME IN ODF FILE *******************
        #
        null_value = np.nan
        if is_in(["depth"], var):
            ncfile_var_list.append(
                NcVar(
                    vartype="depth",
                    varname="depth",
                    varunits="meters",
                    varval=data["data"][var],
                    varclslist=ncfile_var_list,
                    vardim=("z"),
                    varnull=null_value,
                )
            )
        elif is_in(["pressure"], var):
            ncfile_var_list.append(
                NcVar(
                    "pressure",
                    "pressure",
                    "dbar",
                    data["data"][var],
                    ncfile_var_list,
                    ("z"),
                    null_value,
                )
            )
        elif is_in(["temperature"], var):
            ncfile_var_list.append(
                NcVar(
                    "temperature",
                    "temperature",
                    "IPTS-68",
                    data["data"][var],
                    ncfile_var_list,
                    ("z"),
                    null_value,
                )
            )
        elif is_in(["salinity"], var):
            ncfile_var_list.append(
                NcVar(
                    "salinity",
                    "salinity",
                    "PSS-78",
                    data["data"][var],
                    ncfile_var_list,
                    ("z"),
                    null_value,
                )
            )
        else:
            pass
            # print(var, data['metadata']['units'][var], 'not transferred to netcdf file !')
    # now actuallY write the information in CtdNcFile object to a netcdf file
    out.varlist = ncfile_var_list
    # print(ncfile_var_list[0])
    # print('Writing ncfile:',outfile)
    out.write_ncfile(outfile)


# read json file with information on dataset etc.
with open("./config.json", "r") as fid:
    info = json.load(fid)

# read geojson files
polygons_dict = {}
for fname in info["geojsonFileList"]:
    polygons_dict.update(read_geojson(fname))
info.update({"polygons_dict": polygons_dict})
# print(polygons_dict)

flist = glob.glob("./test_files/*.json")
if not os.path.isdir("./temp/"):
    os.mkdir("./temp/")

for f in flist:
    with open(f, "r") as fid:
        data = fid.read()
        data = json.loads(data)
    # parse file
    try:
        print(f)
        write_ctd_ncfile(
            outfile="./temp/{}.nc".format(f.split("/")[-1]),
            odf_data=data,
            **info,
        )
    except Exception as e:
        print("***** ERROR***", f)
        print(e)
