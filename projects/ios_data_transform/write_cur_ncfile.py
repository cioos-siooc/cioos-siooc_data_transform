from datetime import datetime
from pytz import timezone
import json
from cioos_data_transform.OceanNcFile import CurNcFile

# from cioos_data_transform.OceanNcVar import OceanNcVar
from cioos_data_transform.utils import is_in
import numpy as np


def add_ne_speed(speed, direction):
    """
    calculate the North and East speed components of current meter speed and add these variables to
    the output netCDF file
    author: Hana Hourston hana.hourston@dfo-mpo.gc.ca
    input:
        - speed: speed data
        - direction: direction(to) data (measured clockwise from North)
    output:
        None
    """
    east_comp = np.zeros(speed.shape, dtype="float32")
    north_comp = np.zeros(speed.shape, dtype="float32")

    for i in range(len(speed)):
        east_comp[i] = np.round(
            speed[i] * np.cos(np.deg2rad(90 - direction[i])), decimals=3
        )
        north_comp[i] = np.round(
            speed[i] * np.sin(np.deg2rad(90 - direction[i])), decimals=3
        )

    return east_comp, north_comp


def write_cur_ncfile(filename, curcls, config={}):
    """
    use data and methods in curcls object to write the current meter data into a netcdf file
    authors:    Pramod Thupaki pramod.thupaki@hakai.org,
                Hana Hourston hana.hourston@dfo-mpo.gc.ca
    inputs:
        filename: output file name to be created in netcdf format
        curcls: cur object. includes methods to read IOS format and stores data
    output:
        NONE
    """
    # Correct filename to lowercase CUR
    if ".CUR" in filename:
        filename = ".cur".join(filename.rsplit(".CUR", 1))

    date_format = "%Y-%m-%d %H:%M:%S%Z"
    ncfile = CurNcFile()
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
    global_attrs["date_created"] = datetime.now(timezone("UTC")).strftime(
        date_format
    )
    global_attrs["processing_level"] = config.get("processing_level")
    global_attrs["standard_name_vocabulary"] = config.get(
        "standard_name_vocabulary"
    )
    # write full original header, as json dictionary
    global_attrs["header"] = json.dumps(
        curcls.get_complete_header(), ensure_ascii=False, indent=False
    )
    # initcreate dimension variable
    global_attrs["nrec"] = int(curcls.file["NUMBER OF RECORDS"])
    # add variable profile_id (dummy variable)
    global_attrs["filename"] = curcls.filename.split("/")[-1]
    ncfile.add_var("str_id", "filename", None, curcls.filename.split("/")[-1])
    # add administration variables
    if "COUNTRY" in curcls.administration:
        country = curcls.administration["COUNTRY"].strip()
    else:
        country = "n/a"
    global_attrs["country"] = country
    ncfile.add_var("str_id", "country", None, country)
    # create mission id
    if "MISSION" in curcls.administration:
        mission_id = curcls.administration["MISSION"].strip()
    elif "MISSION" in curcls.deployment:
        mission_id = curcls.deployment["MISSION"].strip()
    else:
        mission_id = "n/a"

    if mission_id.lower() == "n/a":
        print("Mission ID not available !", curcls.filename)
    else:
        buf = mission_id.split("-")
        mission_id = "{:4d}-{:03d}".format(int(buf[0]), int(buf[1]))
    global_attrs["mission"] = mission_id
    ncfile.add_var("str_id", "deployment_mission_id", None, mission_id)

    # create event and profile ID
    if "EVENT NUMBER" in curcls.location:
        event_id = curcls.location["EVENT NUMBER"].strip()
    else:
        print("Event number not found!" + curcls.filename)
        event_id = "0000"

    ncfile.add_var("str_id", "event_number", None, event_id)

    if mission_id is None or mission_id == "n/a":
        year_string = curcls.obs_time[0].strftime("%Y")
        profile_id = "{}-000-{:04d}".format(year_string, int(event_id))
    else:
        profile_id = "{:04d}-{:03d}-{:04d}".format(
            int(buf[0]), int(buf[1]), int(event_id)
        )
    # print(profile_id)
    ncfile.add_var("profile", "profile", None, profile_id)
    global_attrs["id"] = profile_id

    if "SCIENTIST" in curcls.administration:
        scientist = curcls.administration["SCIENTIST"].strip()
    else:
        scientist = "n/a"
    global_attrs["scientist"] = scientist
    ncfile.add_var("str_id", "scientist", None, scientist)

    if "PROJECT" in curcls.administration:
        project = curcls.administration["PROJECT"].strip()
    else:
        project = "n/a"
    global_attrs["project"] = project
    ncfile.add_var("str_id", "project", None, project)

    if "AGENCY" in curcls.administration:
        agency = curcls.administration["AGENCY"].strip()
    else:
        agency = "n/a"
    global_attrs["agency"] = agency
    ncfile.add_var("str_id", "agency", None, agency)

    if "PLATFORM" in curcls.administration:
        platform = curcls.administration["PLATFORM"].strip()
    else:
        platform = "n/a"
    global_attrs["platform"] = platform
    ncfile.add_var("str_id", "platform", None, platform)

    # add instrument type
    if "TYPE" in curcls.instrument:
        ncfile.add_var(
            "str_id",
            "instrument_type",
            None,
            curcls.instrument["TYPE"].strip(),
        )

    if "MODEL" in curcls.instrument:
        ncfile.add_var(
            "str_id",
            "instrument_model",
            None,
            curcls.instrument["MODEL"].strip(),
        )

    if "SERIAL NUMBER" in curcls.instrument:
        ncfile.add_var(
            "str_id",
            "instrument_serial_number",
            None,
            curcls.instrument["SERIAL NUMBER"].strip(),
        )

    if "DEPTH" in curcls.instrument:
        ncfile.add_var(
            "instr_depth",
            "instrument_depth",
            None,
            float(curcls.instrument["DEPTH"]),
        )
    # add locations variables
    ncfile.add_var(
        "lat",
        "latitude",
        "degrees_north",
        curcls.location["LATITUDE"],
    )
    global_attrs["geospatial_lat_min"] = curcls.location["LATITUDE"]
    global_attrs["geospatial_lat_max"] = curcls.location["LATITUDE"]
    ncfile.add_var(
        "lon",
        "longitude",
        "degrees_east",
        curcls.location["LONGITUDE"],
    )
    global_attrs["geospatial_lon_min"] = curcls.location["LONGITUDE"]
    global_attrs["geospatial_lon_max"] = curcls.location["LONGITUDE"]
    global_attrs["geospatial_bounds"] = "POINT ({}, {})".format(
        curcls.location["LONGITUDE"], curcls.location["LATITUDE"]
    )

    ncfile.add_var("str_id", "geographic_area", None, curcls.geo_code)

    # add time variables and attributes
    global_attrs["time_coverage_duration"] = str(
        curcls.obs_time[-1] - curcls.obs_time[0]
    )

    global_attrs["time_coverage_resolution"] = str(
        curcls.obs_time[1] - curcls.obs_time[0]
    )

    ncfile.add_var("time", "time", None, curcls.obs_time, vardim=("time"))
    global_attrs["time_coverage_start"] = curcls.obs_time[0].strftime(
        date_format
    )
    global_attrs["time_coverage_end"] = curcls.obs_time[-1].strftime(
        date_format
    )
    # direction_index = None
    # for i, channel in enumerate(curcls.channels['Name']):
    #     if is_in(['direction:geog(to)'], channel):
    #         direction_index = i
    # if direction_index is not None:
    #     if len(curcls.obs_time) <= len(curcls.data[:, direction_index]):
    #         ncfile_var_list.append(OceanNcVar('time', 'time', None, None, None, curcls.obs_time, vardim=('time')))
    #     else:
    #         print('Time range length ({}) greater than direction:geog(to) length ({}) !'.format(
    #             len(curcls.obs_time), len(curcls.data[:, direction_index])))
    #         out.nrec = len(curcls.data[:, direction_index]) #correct number of records
    #         ncfile_var_list.append(OceanNcVar('time', 'time', None, None, None,
    #                                           curcls.obs_time[:len(curcls.data[:, direction_index])], vardim=('time')))

    flag_ne_speed = 0  # flag to determine if north and east speed components are vars in the .cur file
    flag_cndc = 0  # flag to check for conductivity
    flag_cndc_ratio = 0  # flag to check for conductivity ratio
    temp_count = 0  # counter for the number of "Temperature" channels

    # go through channels and add each variable depending on type
    for i, channel in enumerate(curcls.channels["Name"]):
        try:
            null_value = curcls.channel_details["Pad"][i]
        except Exception as e:
            if "PAD" in curcls.file.keys():
                null_value = curcls.file["PAD"].strip()
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
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["pressure"], channel):
            ncfile.add_var(
                "pressure",
                "pressure",
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["temperature:low_res"], channel):
            ncfile.add_var(
                "temperature:cur:low_res",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["temperature"], channel) and not is_in(
            ["temperature:high_res", "temperature:low_res"], channel
        ):
            temp_count += 1
            if temp_count == 1:
                ncfile.add_var(
                    "temperature:cur",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.data[:, i],
                    ("time"),
                    null_value,
                    attributes={"featureType": "timeSeries"},
                )
            elif temp_count == 2:
                ncfile.add_var(
                    "temperature:cur:high_res",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.data[:, i],
                    ("time"),
                    null_value,
                    attributes={"featureType": "timeSeries"},
                )
        elif is_in(["temperature:high_res"], channel):
            ncfile.add_var(
                "temperature:cur:high_res",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )
        elif is_in(["salinity"], channel):
            ncfile.add_var(
                "salinity:cur",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["oxygen"], channel) and not is_in(
            ["flag", "bottle", "rinko", "temperature", "current"], channel
        ):
            ncfile.add_var(
                "oxygen",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["conductivity"], channel) and not is_in(
            ["conductivity_ratio", "conductivity ratio"], channel
        ):
            pass

        elif is_in(["conductivity_ratio", "conductivity ratio"], channel):
            pass

        elif is_in(["speed:east", "ew_comp"], channel):
            flag_ne_speed += 1
            ncfile.add_var(
                "speed:east",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["speed:north", "ns_comp"], channel):
            ncfile.add_var(
                "speed:north",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["speed:up"], channel):
            ncfile.add_var(
                "speed:up",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["amplitude:beam1"], channel):
            ncfile.add_var(
                "amplitude:beam1",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["amplitude:beam2"], channel):
            ncfile.add_var(
                "amplitude:beam2",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["amplitude:beam3"], channel):
            ncfile.add_var(
                "amplitude:beam3",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["speed:sound"], channel) and not is_in(
            ["speed:sound:1", "speed:sound:2"], channel
        ):
            ncfile.add_var(
                "speed:sound",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["speed:sound:1"], channel):
            ncfile.add_var(
                "speed:sound:1",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["speed:sound:2"], channel):
            ncfile.add_var(
                "speed:sound:2",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["heading"], channel):
            ncfile.add_var(
                "heading",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["pitch"], channel):
            ncfile.add_var(
                "pitch",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["roll"], channel):
            ncfile.add_var(
                "roll",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

        elif is_in(["speed"], channel):
            ncfile.add_var(
                "speed",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )
            index_speed = i

        elif is_in(["direction:geog(to)", "direction:current"], channel):
            ncfile.add_var(
                "direction:geog(to)",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )
            index_direction = i

        elif is_in(["sigma-t"], channel):
            ncfile.add_var(
                "sigma-t",
                curcls.channels["Name"][i],
                curcls.channels["Units"][i],
                curcls.data[:, i],
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )
        else:
            print(channel, "not transferred to netcdf file !")

    # Calculate North and East components of speed if missing
    try:
        if flag_ne_speed == 0:
            if (
                type(curcls.data[:, index_speed][0]) is np.bytes_
                and type(curcls.data[:, index_direction][0]) is np.bytes_
            ):
                speed_decoded = np.array(
                    [
                        float(a.decode("ascii"))
                        for a in curcls.data[:, index_speed]
                    ]
                )
                direction_decoded = np.array(
                    [
                        float(a.decode("ascii"))
                        for a in curcls.data[:, index_direction]
                    ]
                )
                speed_east, speed_north = add_ne_speed(
                    speed_decoded, direction_decoded
                )
            else:
                speed_east, speed_north = add_ne_speed(
                    curcls.data[:, index_speed],
                    curcls.data[:, index_direction],
                )

            null_value = "' '"
            ncfile.add_var(
                "speed:east",
                "Speed:East",
                curcls.channels["Units"][index_speed],
                speed_east,
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )

            ncfile.add_var(
                "speed:north",
                "Speed:North",
                curcls.channels["Units"][index_speed],
                speed_north,
                ("time"),
                null_value,
                attributes={"featureType": "timeSeries"},
            )
            print("Calculated east and north speed components ...")
    except UnboundLocalError as e:
        print("Speed and speed component channels not found in file !")

    # attach variables to ncfileclass and call method to write netcdf file
    ncfile.write_ncfile(filename)
    print("Finished writing file:", filename, "\n")
    # release_memory(out)
    return 1
