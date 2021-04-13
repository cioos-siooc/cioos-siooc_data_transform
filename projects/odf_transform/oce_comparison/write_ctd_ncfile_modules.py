import glob
import json
import os
import re
import traceback
from datetime import datetime
import numpy as np
from pytz import timezone
from cioos_data_transform.utils import is_in
from cioos_data_transform.utils import fix_path
from cioos_data_transform.OdfCls import CtdNcFile
from cioos_data_transform.utils import get_geo_code, read_geojson
from cioos_data_transform.utils import oce
from cioos_data_transform.utils import odf

# from odf_transform.utils.utils import get_geo_code, read_geojson
#from utils.oce import get_odf_var_attributes_to_oce


def read_config(config_file):
    # read json file with information on dataset etc.
    with open(config_file) as fid:
        config = json.load(fid)

        # Read Vocabulary file
        for vocab_file in config['vocabularyFileList']:
            config.update({"vocabulary": {}})
            with open(vocab_file) as fid:
                vocab = json.load(fid)
            config["vocabulary"].update(vocab)

        return config


def write_ctd_ncfile(outfile, odf_data, config={}):
    """
    use data and methods in ctdcls object to write the CTD data into a netcdf file
    author:
    inputs:
        outfile: output file name to be created in netcdf format
        odf_data: dict with data from odf file converted to json using oce package
        config: optional arguments, mostly from the config file
    output:
        NONE
    """

    data = odf_data["data"]
    metadata = odf_data["metadata"]

    ncfile = CtdNcFile()
    global_attrs = {}

    ncfile.global_attrs = global_attrs

    # create unique ID for each profile
    profile_id = f"{metadata['cruiseNumber']}-{metadata['eventNumber']}-{metadata['eventQualifier']}"

    # write global attributes

    global_attrs["featureType"] = "profile"

    global_attrs["summary"] = config.get("summary")
    global_attrs["title"] = config.get("title")
    global_attrs["institution"] = metadata.get("institute")
    global_attrs["history"] = ""
    global_attrs["infoUrl"] = config.get("infoUrl")

    # write full original header, as json dictionary
    global_attrs["header"] = json.dumps(
        metadata["header"], ensure_ascii=False, indent=False
    )
    global_attrs["description"] = config.get("description")
    global_attrs["keywords"] = config.get("keywords")
    global_attrs["acknowledgement"] = config.get("acknowledgement")
    global_attrs["id"] = profile_id
    global_attrs["naming_authority"] = "COARDS"
    global_attrs["comment"] = ""
    global_attrs["creator_name"] = config.get("creator_name")
    global_attrs["creator_email"] = config.get("creator_email")
    global_attrs["creator_url"] = config.get("creator_url")
    global_attrs["license"] = config.get("license")
    global_attrs["project"] = metadata["cruise"]
    global_attrs["keywords_vocabulary"] = config.get("keywords_vocabulary")
    global_attrs["Conventions"] = config.get("Conventions")
    global_attrs["cdm_profile_variables"] = "time"

    # initcreate dimension variable

    # use length of first variable to define length of profile
    ncfile.nrec = len(data[list(data.keys())[0]])
    # add variable profile_id (dummy variable)

    ncfile.add_var(
        "str_id",
        "filename",
        None,
        metadata["filename"].split("/")[-1],
    )

    # add administration variables
    ncfile.add_var("str_id", "country", None, "Canada")
    ncfile.add_var("str_id", "cruise_id", None, metadata["cruiseNumber"])
    ncfile.add_var("str_id", "scientist", None, metadata["scientist"])
    ncfile.add_var("str_id", "platform", None, metadata["ship"])
    ncfile.add_var(
        "str_id",
        "instrument_type",
        None,
        metadata["type"] + " " + metadata["model"],
    )

    ncfile.add_var(
        "str_id",
        "instrument_serial_number",
        None,
        metadata["serialNumber"],
    )

    # add locations variables
    ncfile.add_var("lat", "latitude", "degrees_north", metadata["latitude"])

    ncfile.add_var("lon", "longitude", "degrees_east", metadata["longitude"])

    ncfile.add_var(
        "str_id",
        "geographic_area",
        None,
        get_geo_code(
            [
                float(metadata["longitude"]),
                float(metadata["latitude"]),
            ],
            config["polygons_dict"],
        ),
    )
    event_id = f"{metadata['eventQualifier']}-{metadata['eventNumber']}"

    ncfile.add_var("str_id", "event_number", None, event_id)

    print("Profile ID:", profile_id)

    ncfile.add_var("profile", "profile", None, profile_id)
    # pramod - someone should check this...
    date_obj = datetime.utcfromtimestamp(metadata["startTime"])
    date_obj = date_obj.astimezone(timezone("UTC"))
    ncfile.add_var("time", "time", None, [date_obj])

    # Read OCE units and create a unit field in string format
    metadata['units'] = oce.convert_oce_units_to_udunit(metadata['units'])

    # Retrieve ODF Original Variable Attributes by digging in OCE
    odf_variable_attributes = oce.get_odf_variable_attributes(metadata, prefix='original_')

    # Retrieve ODF Original Data (Combine OCE data and Flags)
    odf_data = oce.retrieve_odf_data_from_oce(data, metadata, odf_variable_attributes, 'original_')

    # Retrieve OCE units
    odf_variable_attributes = oce.oce_units_to_odf(odf_variable_attributes, metadata)

    # Add vocabularies and documentation to variable attributes
    odf_variable_attributes = odf.define_odf_variable_attributes(odf_variable_attributes, metadata,
                                                                 organizations=config['organisationVocabulary'],
                                                                 vocabulary=config['vocabulary'])

    # Explicitly use OCE units (where not sure if there's any conversion applied within OCE)
    for var, att in odf_variable_attributes.items():
        if 'original_UNITS' in att:
            att['units'] = att['original_UNITS']

    # Generate BODC Variables based variable, units and instrument
    # TODO add a tool to the derives the different BODC variables based metadata

    # # Generate a variable for each variables available within the ODF files.
    # # TODO we would need to make the add_var method to be able to handle extra attributes
    for var in odf_data.keys():
        if 'units' not in odf_variable_attributes[var]:
            odf_variable_attributes[var]['units'] = None
        odf_variable_attributes[var]

        if 'name' in odf_variable_attributes[var]:
            var_name = odf_variable_attributes[var].pop('name')
        else:
            var_name = var



        ncfile.add_var(
            vartype=odf_variable_attributes[var]['original_TYPE'],
            varname=var_name,
            varunits=odf_variable_attributes[var]['units'],
            varval=odf_data[var],
            vardim=("z"),
            varnull=None,
            conv_to_BODC=False,
            attributes=odf_variable_attributes[var]
        )

    for var in data.keys():
        #
        # ***********  TODO: CREATE A FUNCTION TO CONVERT UNITS FROM DICTIONARY FORMAT TO PLAIN STRING   ************
        # ***********  TODO: DETERMINE BODC/GF3 CODE FROM THE UNITS AND VARIABLE NAME IN ODF FILE *******************
        #
        null_value = np.nan
        if is_in(["depth"], var):
            ncfile.add_var(
                vartype="depth",
                varname="depth",
                varunits="meters",
                varval=data[var],
                vardim=("z"),
                varnull=null_value,
            )

        elif is_in(["pressure"], var):
            ncfile.add_var(
                "pressure",
                "pressure",
                "dbar",
                data[var],
                ("z"),
                null_value,
            )

        elif is_in(["temperature"], var):
            ncfile.add_var(
                "temperature",
                "temperature",
                "IPTS-68",
                data[var],
                ("z"),
                null_value,
            )

        elif is_in(["salinity"], var):
            ncfile.add_var(
                "salinity",
                "salinity",
                "PSS-78",
                data[var],
                ("z"),
                null_value,
            )

        else:
            pass
            # print(var, data['metadata']['units'][var], 'not transferred to netcdf file !')
    # now actuallY write the information in CtdNcFile object to a netcdf file
    # print(ncfile_var_list[0])
    # print('Writing ncfile:',outfile)
    ncfile.write_ncfile(outfile)


def convert_test_files(config):
    flist = glob.glob(config["TEST_FILES_PATH"] + "/*.json")

    if not os.path.isdir(config["TEST_FILES_OUTPUT"]):
        os.mkdir(config["TEST_FILES_OUTPUT"])

    for f in flist:
        with open(f) as fid:
            data = fid.read()
            data = json.loads(data)
        # parse file
        try:
            print(f)
            write_ctd_ncfile(
                outfile=config["TEST_FILES_OUTPUT"]
                + "{}.nc".format(os.path.basename(f)),
                odf_data=data,
                config=config,
            )

        except Exception as e:
            print("***** ERROR***", f)
            print(e)
            print(traceback.print_exc())


def read_geojson_file_list(fileList):
    # read geojson files
    polygons_dict = {}
    for fname in fileList:
        polygons_dict.update(read_geojson(fname))
    return polygons_dict


#
# make this file importable
#
if __name__ == "__main__":
    CONFIG_PATH = fix_path("./config.json")
    TEST_FILES_PATH = fix_path("./test/test_files/")
    TEST_FILES_OUTPUT = fix_path("./test/temp/")

    config = read_config(CONFIG_PATH)
    config.update(
        {"polygons_dict": read_geojson_file_list(config["geojsonFileList"])}
    )
    config.update(
        {
            "TEST_FILES_PATH": TEST_FILES_PATH,
            "TEST_FILES_OUTPUT": TEST_FILES_OUTPUT,
        }
    )

    convert_test_files(config)
