import json
from datetime import datetime

from pytz import timezone

from cioos_data_transform.OceanNcFile import MCtdNcFile

# from cioos_data_transform.OceanNcVar import OceanNcVar
from cioos_data_transform.utils import is_in


def write_mctd_ncfile(filename, ctdcls, config={}):
    """
    use data and methods in ctdcls object to write the CTD data into a netcdf file
    author: Pramod Thupaki pramod.thupaki@hakai.org
    inputs:
        filename: output file name to be created in netcdf format
        ctdcls: ctd object. includes methods to read IOS format and stores data
    output:
        NONE
    """
    date_format = "%Y-%m-%d %H:%M:%S%Z"
    ncfile = MCtdNcFile()
    # write global attributes
    global_attrs = {}
    ncfile.global_attrs = global_attrs
    global_attrs["featureType"] = "timeSeries"
    global_attrs["summary"] = config.get("summary")
    global_attrs["title"] = config.get("title")
    global_attrs["institution"] = config.get("institution")
    global_attrs["infoUrl"] = config.get("infoUrl")
    global_attrs["description"] = config.get("description")
    global_attrs["keywords"] = config.get("keywords")
    global_attrs["acknowledgement"] = config.get("acknowledgement")
    global_attrs["naming_authority"] = "COARDS"
    global_attrs["comment"] = config.get("comment")
    global_attrs["creator_name"] = config.get("creator_name")
    global_attrs["creator_email"] = config.get("creator_email")
    global_attrs["creator_url"] = config.get("creator_url")
    global_attrs["license"] = config.get("license")
    global_attrs["keywords"] = config.get("keywords")
    global_attrs["keywords_vocabulary"] = config.get("keywords_vocabulary")
    global_attrs["Conventions"] = config.get("Conventions")
    global_attrs["cdm_data_type"] = "TimeSeries"
    global_attrs["cdm_timeseries_variables"] = "profile"
    global_attrs["date_created"] = datetime.now(timezone("UTC")).strftime(date_format)
    global_attrs["processing_level"] = config.get("processing_level")
    global_attrs["standard_name_vocabulary"] = config.get("standard_name_vocabulary")
    # write full original header, as json dictionary
    global_attrs["header"] = json.dumps(
        ctdcls.get_complete_header(), ensure_ascii=False, indent=False
    )
    # initcreate dimension variable
    global_attrs["nrec"] = int(ctdcls.file["NUMBER OF RECORDS"])
    # add variable profile_id (dummy variable)
    global_attrs["filename"] = ctdcls.filename.split("/")[-1]
    ncfile.add_var("str_id", "filename", None, ctdcls.filename.split("/")[-1])

    # add administration variables
    if "COUNTRY" in ctdcls.administration:
        country = ctdcls.administration["COUNTRY"].strip()
    else:
        country = "n/a"
    ncfile.add_var("str_id", "country", None, country)
    global_attrs["country"] = country

    # create mission id
    if "MISSION" in ctdcls.administration:
        mission_id = ctdcls.administration["MISSION"].strip()
    elif "MISSION" in ctdcls.deployment:
        mission_id = ctdcls.deployment["MISSION"].strip()
    else:
        mission_id = None

    if mission_id is None:
        # raise Exception("Error: Mission ID not available", ctdcls.filename)
        print("Mission ID not available !", ctdcls.filename)
        buf = [ctdcls.start_date[:4], "000"]
    else:
        buf = mission_id.split("-")

    mission_id = "{:4d}-{:03d}".format(int(buf[0]), int(buf[1]))
    global_attrs["mission"] = mission_id
    ncfile.add_var("str_id", "mission_id", None, mission_id)

    if "SCIENTIST" in ctdcls.administration:
        scientist = ctdcls.administration["SCIENTIST"].strip()
    else:
        scientist = "n/a"
    global_attrs["scientist"] = scientist
    ncfile.add_var("str_id", "scientist", None, scientist)

    if "PROJECT" in ctdcls.administration:
        project = ctdcls.administration["PROJECT"].strip()
    else:
        project = "n/a"
    global_attrs["project"] = project
    ncfile.add_var("str_id", "project", None, project)

    if "AGENCY" in ctdcls.administration:
        agency = ctdcls.administration["AGENCY"].strip()
    else:
        agency = "n/a"
    global_attrs["agency"] = agency
    ncfile.add_var("str_id", "agency", None, agency)

    if "PLATFORM" in ctdcls.administration:
        platform = ctdcls.administration["PLATFORM"].strip()
    else:
        platform = "n/a"
    global_attrs["platform"] = platform
    ncfile.add_var("str_id", "platform", None, platform)

    # add instrument type
    if "TYPE" in ctdcls.instrument:
        ncfile.add_var(
            "str_id",
            "instrument_type",
            None,
            ctdcls.instrument["TYPE"].strip(),
        )

    if "MODEL" in ctdcls.instrument:
        ncfile.add_var(
            "str_id",
            "instrument_model",
            None,
            ctdcls.instrument["MODEL"].strip(),
        )

    if "SERIAL NUMBER" in ctdcls.instrument:
        ncfile.add_var(
            "str_id",
            "instrument_serial_number",
            None,
            ctdcls.instrument["SERIAL NUMBER"].strip(),
        )

    if "DEPTH" in ctdcls.instrument:
        ncfile.add_var(
            "instr_depth",
            "instrument_depth",
            None,
            float(ctdcls.instrument["DEPTH"]),
        )
    # add locations variables
    ncfile.add_var(
        "lat",
        "latitude",
        "degrees_north",
        ctdcls.location["LATITUDE"],
    )
    global_attrs["geospatial_lat_min"] = ctdcls.location["LATITUDE"]
    global_attrs["geospatial_lat_max"] = ctdcls.location["LATITUDE"]
    ncfile.add_var(
        "lon",
        "longitude",
        "degrees_east",
        ctdcls.location["LONGITUDE"],
    )
    global_attrs["geospatial_lon_min"] = ctdcls.location["LONGITUDE"]
    global_attrs["geospatial_lon_max"] = ctdcls.location["LONGITUDE"]
    global_attrs["geospatial_bounds"] = "POINT ({}, {})".format(
        ctdcls.location["LONGITUDE"], ctdcls.location["LATITUDE"]
    )

    ncfile.add_var("str_id", "geographic_area", None, ctdcls.geo_code)

    if "EVENT NUMBER" in ctdcls.location:
        event_id = ctdcls.location["EVENT NUMBER"].strip()
    else:
        print('Unable to guess event_id from file name. Using "0000" !')
        event_id = "0000"

    ncfile.add_var("str_id", "event_number", None, event_id)
    profile_id = "{:04d}-{:03d}-{:04d}".format(int(buf[0]), int(buf[1]), int(event_id))
    # print(profile_id)
    ncfile.add_var(
        "profile",
        "profile",
        None,
        profile_id,
        attributes={"cf_role": "timeSeries_id"},
    )
    global_attrs["id"] = profile_id
    # add time variable
    global_attrs["time_coverage_duration"] = str(
        ctdcls.obs_time[-1] - ctdcls.obs_time[0]
    )
    global_attrs["time_coverage_resolution"] = str(
        ctdcls.obs_time[1] - ctdcls.obs_time[0]
    )
    ncfile.add_var("time", "time", None, ctdcls.obs_time, vardim=("time"))
    global_attrs["time_coverage_start"] = ctdcls.obs_time[0].strftime(date_format)
    global_attrs["time_coverage_end"] = ctdcls.obs_time[-1].strftime(date_format)

    # go through channels and add each variable depending on type
    for i, channel in enumerate(ctdcls.channels["Name"]):
        try:
            null_value = ctdcls.channel_details["Pad"][i]
        except Exception as e:
            print(e)
            if "PAD" in ctdcls.file.keys():
                null_value = ctdcls.file["PAD"].strip()
                print(
                    "Channel Details missing. Setting Pad value to: ",
                    null_value.strip(),
                )
            else:
                print("Channel Details missing. Setting Pad value to ' ' ...")
                null_value = "' '"
        if is_in(["depth"], channel):
            ncfile.add_var(
                "depth",
                "depth",
                ctdcls.channels["Units"][i],
                ctdcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["pressure"], channel):
            ncfile.add_var(
                "pressure",
                "pressure",
                ctdcls.channels["Units"][i],
                ctdcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["temperature"], channel) and not is_in(["flag", "bottle"], channel):
            ncfile.add_var(
                "temperature",
                ctdcls.channels["Name"][i],
                ctdcls.channels["Units"][i],
                ctdcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["salinity"], channel) and not is_in(["flag", "bottle"], channel):
            ncfile.add_var(
                "salinity",
                ctdcls.channels["Name"][i],
                ctdcls.channels["Units"][i],
                ctdcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )
        elif is_in(["oxygen"], channel) and not is_in(
            ["flag", "bottle", "rinko", "temperature", "current"], channel
        ):
            ncfile.add_var(
                "oxygen",
                ctdcls.channels["Name"][i],
                ctdcls.channels["Units"][i],
                ctdcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )
        elif is_in(["conductivity"], channel):
            ncfile.add_var(
                "conductivity",
                ctdcls.channels["Name"][i],
                ctdcls.channels["Units"][i],
                ctdcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )
        else:
            print(channel, "not transferred to netcdf file !")
            # raise Exception('not found !!')

    # attach variables to ncfileclass and call method to write netcdf file
    ncfile.write_ncfile(filename)
    print("Finished writing file:", filename, "\n")
    # release_memory(out)
    return 1
