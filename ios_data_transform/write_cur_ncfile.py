import json
from cioos_data_transform.OceanNcFile import CurNcFile
from cioos_data_transform.OceanNcVar import OceanNcVar
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


def write_cur_ncfile(filename, curcls):
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

    out = CurNcFile()
    # write global attributes
    out.featureType = "timeSeries"
    out.summary = "This dataset contains observations made by the Institute of Ocean Sciences of Fisheries and Oceans (DFO) using current meters mounted on moorings."
    out.title = "This dataset contains observations made by the Institute of Ocean Sciences of Fisheries and Oceans (DFO) using current meters mounted on moorings."
    out.institution = "Institute of Ocean Sciences, 9860 West Saanich Road, Sidney, B.C., Canada"
    out.infoUrl = "http://www.pac.dfo-mpo.gc.ca/science/oceans/data-donnees/index-eng.html"
    out.cdm_profile_variables = "time"  # TEMPS901, TEMPS902, TEMPS601, TEMPS602, TEMPS01, PSALST01, PSALST02, PSALSTPPT01, PRESPR01
    # write full original header, as json dictionary
    out.HEADER = json.dumps(
        curcls.get_complete_header(), ensure_ascii=False, indent=False
    )
    # initcreate dimension variable
    out.nrec = int(curcls.file["NUMBER OF RECORDS"])
    # add variable profile_id (dummy variable)
    ncfile_var_list = []
    # profile_id = random.randint(1, 100000)
    ncfile_var_list.append(
        OceanNcVar(
            "str_id",
            "filename",
            None,
            None,
            None,
            curcls.filename.split("/")[-1],
        )
    )
    # add administration variables
    if "COUNTRY" in curcls.administration:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "country",
                None,
                None,
                None,
                curcls.administration["COUNTRY"].strip(),
            )
        )
    if "MISSION" in curcls.administration:
        mission_id = curcls.administration["MISSION"].strip()
    else:
        mission_id = "n/a"

    if mission_id.lower() == "n/a":
        # raise Exception("Error: Mission ID not available", curcls.filename)
        # print("Mission ID not available !", curcls.filename)
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "deployment_mission_id",
                None,
                None,
                None,
                mission_id.lower(),
            )
        )
    else:
        buf = mission_id.split("-")
        mission_id = "{:4d}-{:03d}".format(int(buf[0]), int(buf[1]))
        ncfile_var_list.append(
            OceanNcVar(
                "str_id", "deployment_mission_id", None, None, None, mission_id
            )
        )

    if "SCIENTIST" in curcls.administration:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "scientist",
                None,
                None,
                None,
                curcls.administration["SCIENTIST"].strip(),
            )
        )
    if "PROJECT" in curcls.administration:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "project",
                None,
                None,
                None,
                curcls.administration["PROJECT"].strip(),
            )
        )
    if "AGENCY" in curcls.administration:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "agency",
                None,
                None,
                None,
                curcls.administration["AGENCY"].strip(),
            )
        )
    if "PLATFORM" in curcls.administration:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "platform",
                None,
                None,
                None,
                curcls.administration["PLATFORM"].strip(),
            )
        )
    # add instrument type
    if "TYPE" in curcls.instrument:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "instrument_type",
                None,
                None,
                None,
                curcls.instrument["TYPE"].strip(),
            )
        )
    if "MODEL" in curcls.instrument:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "instrument_model",
                None,
                None,
                None,
                curcls.instrument["MODEL"].strip(),
            )
        )
    if "SERIAL NUMBER" in curcls.instrument:
        ncfile_var_list.append(
            OceanNcVar(
                "str_id",
                "instrument_serial_number",
                None,
                None,
                None,
                curcls.instrument["SERIAL NUMBER"].strip(),
            )
        )
    if "DEPTH" in curcls.instrument:
        ncfile_var_list.append(
            OceanNcVar(
                "instr_depth",
                "instrument_depth",
                None,
                None,
                None,
                float(curcls.instrument["DEPTH"]),
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
            curcls.location["LATITUDE"],
        )
    )
    ncfile_var_list.append(
        OceanNcVar(
            "lon",
            "longitude",
            "degrees_east",
            None,
            None,
            curcls.location["LONGITUDE"],
        )
    )
    ncfile_var_list.append(
        OceanNcVar(
            "str_id", "geographic_area", None, None, None, curcls.geo_code
        )
    )

    if "EVENT NUMBER" in curcls.location:
        event_id = curcls.location["EVENT NUMBER"].strip()
    else:
        # print("Event number not found!" + curcls.filename)
        event_id = "0000"
    ncfile_var_list.append(
        OceanNcVar("str_id", "event_number", None, None, None, event_id)
    )
    # add time variable
    if mission_id.lower() == "n/a":
        profile_id = "{}-{:04d}".format(mission_id.lower(), int(event_id))
    else:
        profile_id = "{:04d}-{:03d}-{:04d}".format(
            int(buf[0]), int(buf[1]), int(event_id)
        )

    # print(profile_id)
    ncfile_var_list.append(
        OceanNcVar("profile", "profile", None, None, None, profile_id)
    )

    # Check if variable lengths are same length as curcls.obs_time
    ncfile_var_list.append(
        OceanNcVar(
            "time", "time", None, None, None, curcls.obs_time, vardim=("time")
        )
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
            ncfile_var_list.append(
                OceanNcVar(
                    "depth",
                    "depth",
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["pressure"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "pressure",
                    "pressure",
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["temperature:low_res"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "temperature:cur:low_res",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["temperature"], channel) and not is_in(
            ["temperature:high_res", "temperature:low_res"], channel
        ):
            temp_count += 1
            if temp_count == 1:
                ncfile_var_list.append(
                    OceanNcVar(
                        "temperature:cur",
                        curcls.channels["Name"][i],
                        curcls.channels["Units"][i],
                        curcls.channels["Minimum"][i],
                        curcls.channels["Maximum"][i],
                        curcls.data[:, i],
                        ncfile_var_list,
                        ("time"),
                        null_value,
                    )
                )
            elif temp_count == 2:
                ncfile_var_list.append(
                    OceanNcVar(
                        "temperature:cur:high_res",
                        curcls.channels["Name"][i],
                        curcls.channels["Units"][i],
                        curcls.channels["Minimum"][i],
                        curcls.channels["Maximum"][i],
                        curcls.data[:, i],
                        ncfile_var_list,
                        ("time"),
                        null_value,
                    )
                )
        elif is_in(["temperature:high_res"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "temperature:cur:high_res",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["salinity"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "salinity:cur",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["oxygen"], channel) and not is_in(
            ["flag", "bottle", "rinko", "temperature", "current"], channel
        ):
            ncfile_var_list.append(
                OceanNcVar(
                    "oxygen",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["conductivity"], channel) and not is_in(
            ["conductivity_ratio", "conductivity ratio"], channel
        ):
            pass
        elif is_in(["conductivity_ratio", "conductivity ratio"], channel):
            pass
        elif is_in(["speed:east", "ew_comp"], channel):
            flag_ne_speed += 1
            ncfile_var_list.append(
                OceanNcVar(
                    "speed:east",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["speed:north", "ns_comp"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "speed:north",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["speed:up"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "speed:up",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["amplitude:beam1"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "amplitude:beam1",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["amplitude:beam2"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "amplitude:beam2",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["amplitude:beam3"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "amplitude:beam3",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["speed:sound"], channel) and not is_in(
            ["speed:sound:1", "speed:sound:2"], channel
        ):
            ncfile_var_list.append(
                OceanNcVar(
                    "speed:sound",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["speed:sound:1"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "speed:sound:1",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["speed:sound:2"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "speed:sound:2",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["heading"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "heading",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["pitch"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "pitch",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["roll"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "roll",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        elif is_in(["speed"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "speed",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
            index_speed = i
        elif is_in(["direction:geog(to)", "direction:current"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "direction:geog(to)",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
            index_direction = i
        elif is_in(["sigma-t"], channel):
            ncfile_var_list.append(
                OceanNcVar(
                    "sigma-t",
                    curcls.channels["Name"][i],
                    curcls.channels["Units"][i],
                    curcls.channels["Minimum"][i],
                    curcls.channels["Maximum"][i],
                    curcls.data[:, i],
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
        else:
            print(channel, "not transferred to netcdf file !")
            # raise Exception('not found !!')

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
            ncfile_var_list.append(
                OceanNcVar(
                    "speed:east",
                    "Speed:East",
                    curcls.channels["Units"][index_speed],
                    np.nanmin(speed_east),
                    np.nanmax(speed_east),
                    speed_east,
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
            ncfile_var_list.append(
                OceanNcVar(
                    "speed:north",
                    "Speed:North",
                    curcls.channels["Units"][index_speed],
                    np.nanmin(speed_north),
                    np.nanmax(speed_north),
                    speed_north,
                    ncfile_var_list,
                    ("time"),
                    null_value,
                )
            )
            print("Calculated east and north speed components ...")
    except UnboundLocalError as e:
        print("Speed and speed component channels not found in file !")

    # attach variables to ncfileclass and call method to write netcdf file
    out.varlist = ncfile_var_list
    out.write_ncfile(filename)
    print("Finished writing file:", filename, "\n")
    # release_memory(out)
    return 1
