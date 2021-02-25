# import random
import json
from cioos_data_transform.OceanNcFile import CtdNcFile
from cioos_data_transform.OceanNcVar import OceanNcVar
from cioos_data_transform.utils import is_in


def write_ctd_ncfile(filename, ctdcls):
    """
    use data and methods in ctdcls object to write the CTD data into a netcdf file
    author: Pramod Thupaki pramod.thupaki@hakai.org
    inputs:
        filename: output file name to be created in netcdf format
        ctdcls: ctd object. includes methods to read IOS format and stores data
    output:
        NONE
    """
    out = CtdNcFile()
    # write global attributes
    out.featureType = "profile"
    if ctdcls.type == "ctd":
        out.summary = "This dataset contains observations made by the Institute of Ocean Sciences of Fisheries and Oceans (DFO) using CTDs mounted on rosettes."
        out.title = "This dataset contains observations made by the Institute of Ocean Sciences of Fisheries and Oceans (DFO) using CTDs mounted on rosettes."
    elif ctdcls.type == "bot":
        out.summary = "This dataset contains observations made by the Institute of Ocean Sciences of Fisheries and Oceans (DFO) using water samples."
        out.title = "This dataset contains observations made by the Institute of Ocean Sciences of Fisheries and Oceans (DFO) using water samples."
    else:
        raise Exception("file type not identified !")
    out.institution = "Institute of Ocean Sciences, 9860 West Saanich Road, Sidney, B.C., Canada"
    out.infoUrl = "http://www.pac.dfo-mpo.gc.ca/science/oceans/data-donnees/index-eng.html"
    out.cdm_profile_variables = "time"  # TEMPS901, TEMPS902, TEMPS601, TEMPS602, TEMPS01, PSALST01, PSALST02, PSALSTPPT01, PRESPR01
    # write full original header, as json dictionary
    out.HEADER = json.dumps(
        ctdcls.get_complete_header(), ensure_ascii=False, indent=False
    )
    # initcreate dimension variable
    out.nrec = int(ctdcls.file["NUMBER OF RECORDS"])
    # add variable profile_id (dummy variable)
    ncfile_var_list = []
    ncfile_var_list.append(
        OceanNcVar(
            "str_id",
            "filename",
            None,
            None,
            None,
            ctdcls.filename.split("/")[-1],
        )
    )
    # add administration variables
    if "COUNTRY" in ctdcls.administration:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "country",
                None,
                None,
                None,
                ctdcls.administration["COUNTRY"].strip(),
            )
        )
    if "MISSION" in ctdcls.administration:
        mission_id = ctdcls.administration["MISSION"].strip()
    else:
        mission_id = ctdcls.administration["CRUISE"].strip()
    buf = mission_id.split("-")
    mission_id = "{:04d}-{:03d}".format(int(buf[0]), int(buf[1]))
    ncfile_var_list.append(
        OceanNcVar("str_id", "mission_id", None, None, None, mission_id)
    )
    if "SCIENTIST" in ctdcls.administration:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "scientist",
                None,
                None,
                None,
                ctdcls.administration["SCIENTIST"].strip(),
            )
        )
    if "PROJECT" in ctdcls.administration:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "project",
                None,
                None,
                None,
                ctdcls.administration["PROJECT"].strip(),
            )
        )
    if "AGENCY" in ctdcls.administration:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "agency",
                None,
                None,
                None,
                ctdcls.administration["AGENCY"].strip(),
            )
        )
    if "PLATFORM" in ctdcls.administration:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "platform",
                None,
                None,
                None,
                ctdcls.administration["PLATFORM"].strip(),
            )
        )
    # add instrument type
    if "TYPE" in ctdcls.instrument:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "instrument_type",
                None,
                None,
                None,
                ctdcls.instrument["TYPE"].strip(),
            )
        )
    if "MODEL" in ctdcls.instrument:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "instrument_model",
                None,
                None,
                None,
                ctdcls.instrument["MODEL"].strip(),
            )
        )
    if "SERIAL NUMBER" in ctdcls.instrument:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "instrument_serial_number",
                None,
                None,
                None,
                ctdcls.instrument["SERIAL NUMBER"].strip(),
            )
        )
    # add locations variables
    ncfile_var_list.append(
        OceanNcVar(
            "lat",
            "latitude",
            "degrees_north",
            None,
            None,
            ctdcls.location["LATITUDE"],
        )
    )
    ncfile_var_list.append(
        OceanNcVar(
            "lon",
            "longitude",
            "degrees_east",
            None,
            None,
            ctdcls.location["LONGITUDE"],
        )
    )
    ncfile_var_list.append(
        OceanNcVar(
            "str_id", "geographic_area", None, None, None, ctdcls.geo_code
        )
    )
    if "EVENT NUMBER" in ctdcls.location:
        event_id = ctdcls.location["EVENT NUMBER"].strip()
    else:
        print("Event number not found!" + ctdcls.filename)
        try:
            event_id = ctdcls.filename.split("-")[-1][:-4]
            print("Guessing ...", ctdcls.filename, "; event id = ", event_id)
        except Exception as e:
            print('Unable to guess event_id from file name. Using "0000" !')
            event_id = "0000"

    ncfile_var_list.append(
        OceanNcVar("str_id", "event_number", None, None, None, event_id)
    )
    # add time variable
    profile_id = "{:04d}-{:03d}-{}".format(
        int(buf[0]), int(buf[1]), event_id.zfill(4)
    )
    # print(profile_id)
    ncfile_var_list.append(
        OceanNcVar("profile", "profile", None, None, None, profile_id)
    )
    ncfile_var_list.append(
        OceanNcVar("time", "time", None, None, None, [ctdcls.start_dateobj])
    )
    # go through channels and add each variable depending on type
    for i, channel in enumerate(ctdcls.channels["Name"]):
        try:
            null_value = ctdcls.channel_details["Pad"][i]
        except Exception as e:
            if "PAD" in ctdcls.file.keys():
                null_value = ctdcls.file["PAD"].strip()
                print(
                    "Channel Details missing. Setting Pad value to: ",
                    null_value.strip(),
                )
            else:
                print("Channel Details missing. Setting Pad value to ' ' ...")
                null_value = "' '"
        if is_in(["depth"], channel) and not is_in(["nominal"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "depth",
                    "depth",
                    ctdcls.channels["Units"][i],
                    ctdcls.channels["Minimum"][i],
                    ctdcls.channels["Maximum"][i],
                    ctdcls.data[:, i],
                    ncfile_var_list,
                    ("z"),
                    null_value,
                )
            )
        elif is_in(["pressure"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "pressure",
                    "pressure",
                    ctdcls.channels["Units"][i],
                    ctdcls.channels["Minimum"][i],
                    ctdcls.channels["Maximum"][i],
                    ctdcls.data[:, i],
                    ncfile_var_list,
                    ("z"),
                    null_value,
                )
            )
        elif is_in(["temperature"], channel) and not is_in(
            ["flag", "rinko", "bottle"], channel
        ):
            ncfile_var_list.append(
                OceanNcVar(
                    "temperature",
                    ctdcls.channels["Name"][i],
                    ctdcls.channels["Units"][i],
                    ctdcls.channels["Minimum"][i],
                    ctdcls.channels["Maximum"][i],
                    ctdcls.data[:, i],
                    ncfile_var_list,
                    ("z"),
                    null_value,
                )
            )
        elif is_in(["salinity"], channel) and not is_in(["flag"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "salinity",
                    ctdcls.channels["Name"][i],
                    ctdcls.channels["Units"][i],
                    ctdcls.channels["Minimum"][i],
                    ctdcls.channels["Maximum"][i],
                    ctdcls.data[:, i],
                    ncfile_var_list,
                    ("z"),
                    null_value,
                )
            )
        elif is_in(["oxygen"], channel) and not is_in(
            [
                "flag",
                "bottle",
                "rinko",
                "temperature",
                "current",
                "isotope",
                "saturation",
                "voltage",
            ],
            channel,
        ):
            ncfile_var_list.append(
                OceanNcVar(
                    "oxygen",
                    ctdcls.channels["Name"][i],
                    ctdcls.channels["Units"][i],
                    ctdcls.channels["Minimum"][i],
                    ctdcls.channels["Maximum"][i],
                    ctdcls.data[:, i],
                    ncfile_var_list,
                    ("z"),
                    null_value,
                )
            )
        elif is_in(["conductivity"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "conductivity",
                    ctdcls.channels["Name"][i],
                    ctdcls.channels["Units"][i],
                    ctdcls.channels["Minimum"][i],
                    ctdcls.channels["Maximum"][i],
                    ctdcls.data[:, i],
                    ncfile_var_list,
                    ("z"),
                    null_value,
                )
            )
        #     Nutrients in bottle files
        elif is_in(
            ["nitrate_plus_nitrite", "silicate", "phosphate"], channel
        ) and not is_in(["flag"], channel):
            try:
                ncfile_var_list.append(
                    OceanNcVar(
                        "nutrient",
                        ctdcls.channels["Name"][i],
                        ctdcls.channels["Units"][i],
                        ctdcls.channels["Minimum"][i],
                        ctdcls.channels["Maximum"][i],
                        ctdcls.data[:, i],
                        ncfile_var_list,
                        ("z"),
                        null_value,
                    )
                )
            except Exception as e:
                print(e)
        #  Chlorophyll
        elif is_in(["chlorophyll:extracted"], channel) and not is_in(
            ["flag"], channel
        ):
            try:
                ncfile_var_list.append(
                    OceanNcVar(
                        "other",
                        ctdcls.channels["Name"][i],
                        ctdcls.channels["Units"][i],
                        ctdcls.channels["Minimum"][i],
                        ctdcls.channels["Maximum"][i],
                        ctdcls.data[:, i],
                        ncfile_var_list,
                        ("z"),
                        null_value,
                    )
                )
            except Exception as e:
                print(e)
        else:
            print(
                channel,
                ctdcls.channels["Units"][i],
                "not transferred to netcdf file !",
            )
            # raise Exception('not found !!')

    # attach variables to ncfileclass and call method to write netcdf file
    out.varlist = ncfile_var_list
    out.write_ncfile(filename)
    print("Finished writing file:", filename, "\n")
    # release_memory(out)
    return 1
