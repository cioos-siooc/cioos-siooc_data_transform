"""
    Python class to read IOS data files and store data for conversion to netcdf format
    Changelog Version 0.1: July 15 2019 - convert python scripts and functions into a python class
    Author: Pramod Thupaki (pramod.thupaki@hakai.org)
"""
import struct
from datetime import datetime, timedelta
import re
import fortranformat as ff
import numpy as np
import json

import pkg_resources

from pytz import timezone
from .utils.utils import find_geographic_area, read_geojson, read_ios_vocabulary
from shapely.geometry import Point
import pandas as pd
from io import StringIO
import logging

VERSION = pkg_resources.require("cioos_data_transform")[0].version
logger = logging.getLogger(__name__)
logger = logging.LoggerAdapter(logger, {"file": None})

ios_dtypes_to_python = {
    "R": "float32",
    "F": "float32",
    "I": "int32",
    "D": str,
    "T": str,
    "E": "float32",
}


def get_dtype_from_ios_type(ios_type):
    if not ios_type or ios_type.strip() == "":
        return
    elif ios_type in ios_dtypes_to_python:
        return ios_dtypes_to_python[ios_type]
    elif ios_type[0].upper() in ios_dtypes_to_python:
        return ios_dtypes_to_python[ios_type[0]]


def get_dtype_from_ios_name(ios_name):
    if re.search("flag", ios_name, re.IGNORECASE):
        return "int32"
    elif re.search("time|date", ios_name, re.IGNORECASE):
        return str
    else:
        return float


class ObsFile(object):
    """
    Class template for all the different data file types
    Contains data from the IOS file and methods to read the IOS format
    Specific improvements/modifications required to read filetypes will be make in derived classes
    Author: Pramod Thupaki pramod.thupaki@hakai.org
    Incorporates functions from earlier versions of this toolbox
    """

    def __init__(self, filename, debug):
        # initializes object by reading *FILE and ios_header_version
        # reads entire file to memory for all subsequent processing
        # inputs are filename and debug state

        logger.extra["file"] = filename
        if debug:
            logger.setLevel("DEBUG")

        self.type = None
        self.debug = debug
        self.filename = filename
        self.start_date = None
        self.start_dateobj = None
        self.location = None
        self.channels = None
        self.comments = None
        self.remarks = None
        self.channel_details = None
        self.administration = None
        self.instrument = None
        self.data = None
        self.deployment = None
        self.recovery = None
        self.obs_time = None
        self.vocabulary_attributes = None
        # try opening and reading the file. if error. soft-exit.
        try:
            with open(self.filename, "r", encoding="ASCII", errors="ignore") as fid:
                self.lines = [l for l in fid.readlines()]
            self.ios_header_version = self.get_header_version()
            self.date_created = self.get_date_created()
            self.file = self.get_section("FILE")
            self.status = 1
        except Exception as e:
            logger.error("Unable to open file", filename, exc_info=True)
            self.status = 0
            exit(0)

    def import_data(self):
        pass

    def get_date_created(self):
        return pd.to_datetime(self.lines[0][1:], utc=True)

    def get_header_version(self):
        # reads header version
        return self.lines[self.find_index("*IOS HEADER VERSION")][20:24]

    def find_index(self, string):
        # finds line number that starts with string
        # input: string (nominally the section)
        for i, l in enumerate(self.lines):
            if l.lstrip()[0 : len(string)] == string:
                return i

        logger.debug("Index not found %s", string)
        return -1

    def get_complete_header(self):
        # return all sections in header as a dict
        sections = self.get_list_of_sections()
        header = {}
        for sec in sections:
            # logger.info ("getting section:", sec)
            if sec in ["COMMENTS", "REMARKS", "HISTORY"]:
                header[sec] = self.get_comments_like(sec)
            else:
                header[sec] = self.get_section(sec)
        return header

    def get_section(self, section_name):
        # deciphers the information in a particular section
        # reads table information
        # returns information as dictionary.
        # records (subsections) are returned as list of lines for subsequent processing
        if section_name[0] != "*":
            section_name = "*" + section_name
        idx = self.find_index(section_name)
        if idx == -1:
            logger.info("Section not found" + section_name + self.filename)
            return {}
        info = {}
        # EOS = False # end of section logical
        while True:
            idx += 1
            l = self.lines[idx]
            if len(l.strip()) == 0:  # skip line if blank
                continue
            elif l[0] == "!":
                continue
            elif l[0] in ["$", "*"]:
                break
            elif "$" in l[1:5]:
                # read record or 'sub-section'. This nominally starts with tab of 4 spaces
                # but can be 1 or 2 spaces as well for REMARKS
                EOR = False
                record_name = l.strip()

                logger.debug(
                    "Found subsection:%s in section:%s", record_name, section_name
                )
                info[record_name] = []
                while not EOR:
                    idx += 1
                    l = self.lines[idx]
                    if l.strip()[0:4] == "$END":
                        EOR = True
                    else:
                        info[record_name].append(l)
            else:
                logger.debug(l)
                if len(l.split(":", 1)) > 1:
                    info[l.split(":", 1)[0].strip()] = l.split(":", 1)[1]
        return info

    def get_flag_convention(self, name, units=None):
        if name.lower() == "flag:at_sea":
            return {
                "rename": "flag:at_sea",
                "flag_values": [0, 1, 2, 3, 4, 5],
                "flag_meanings": "not_classified good_at_sea_freely_floating bad_at_sea_but_trapped_in_rocky_intertidal bad_on_land bad:at_sea bad_land_travel",
                "units": None,
            }
        elif units.lower() == "igoss_flags":
            return {
                "flag_values": [0, 1, 2, 3, 4, 5],
                "flag_meanings": "not_checked appears_to_be_good inconsistent_with_climatology appears_to_be_doubtful appears_to_be_wrong value_was_changed_see_history_record",
            }
        elif name.lower() == "flag:ctd" or name.lower() == "flag":
            return {
                "flag_values": [0, 2, 6],
                "flag_meanings": "not_quality_control good interpolated_or_replaced_by_dual_sensor_or_upcast_value",
            }

        logger.warning("Unknown flag name=%s, units=%s", name, units)
        return {}

    def get_file_extension(self):
        if "." in self.filename:
            return self.filename.rsplit(".", 1)[1]
        return None

    def get_subsection(self, name, section):
        # return subsection information from a section
        # used as interface for data actually read into dictionary by get_section
        # provides some useful debug information
        # returns lines that make up 'subsection' if all is well
        info = None
        if name[0] != "$":
            logger.debug("Finding subsection %s", name)
            name = "$" + name
        if name not in self.file.keys():
            logger.warning("Did not find subsection:%s in %s", name, self.filename)
        elif name == "$TABLE: CHANNELS":
            info = self.file[name]
        elif name == "$TABLE: CHANNEL DETAIL":
            info = self.file[name]
        return info

    def get_dt(self):
        # converts time increment from ios format to seconds
        # float32 accurate (seconds are not rounded to integers)
        if "TIME INCREMENT" in self.file and "n/a" not in self.file["TIME INCREMENT"]:
            line = self.file["TIME INCREMENT"]
            dt = np.asarray(line.split("!")[0].split(), dtype=float)
            dt = sum(dt * [24.0 * 3600.0, 3600.0, 60.0, 1.0, 0.001])  # in seconds
        else:
            logger.warning("Time Increment not found in Section:FILE")
            dt = None
        return dt

    def get_date(self, opt="start"):
        # reads datetime string in "START TIME" and converts to datetime object
        # return datetime object and as standard string format
        # read 'END TIME' if opt is 'end'
        if "START TIME" not in self.file:
            raise Exception("START TIME: not available in file", self.filename)

        if opt.lower() == "start":
            date_string = self.file["START TIME"].strip().upper()
        elif opt.lower() == "end":
            date_string = self.file["END TIME"].strip().upper()
        else:
            raise Exception("Invalid option for get_date function !")
        logger.debug("Raw date string: %s", date_string)
        # get the naive (timezone unaware) datetime obj
        try:
            date_obj = datetime.strptime(date_string[4:], "%Y/%m/%d %H:%M:%S.%f")
        except Exception as e:
            logger.warning(e)
            date_obj = datetime.strptime(date_string[4:], "%Y/%m/%d")
            logger.info(date_obj)
        # make datetime object, aware of its timezone
        # for GMT, UTC
        if any([date_string.find(z) == 0 for z in ["GMT", "UTC"]]):
            date_obj = timezone(date_string[0:3]).localize(date_obj)
        # for PST/PDT
        elif "PST" in date_string.upper():
            date_obj = timezone("UTC").localize(date_obj + timedelta(hours=8))
        elif "PDT" in date_string.upper():
            date_obj = timezone("UTC").localize(date_obj + timedelta(hours=7))
        # Canada/Mountain
        elif "MST" in date_string.upper():
            date_obj = timezone("UTC").localize(date_obj + timedelta(hours=7))
        elif "MDT" in date_string.upper():
            date_obj = timezone("UTC").localize(date_obj + timedelta(hours=6))
        # Canada/Atlantic
        elif "AST" in date_string.upper():
            date_obj = timezone("UTC").localize(date_obj + timedelta(hours=4))
        elif "ADT" in date_string.upper():
            date_obj = timezone("UTC").localize(date_obj + timedelta(hours=3))
        else:
            raise Exception("Problem finding the timezone information->", self.filename)

        logger.debug("Date obj with timezone info: %s", date_obj)
        # convert all datetime to utc before writing to netcdf file
        date_obj = date_obj.astimezone(timezone("UTC"))
        return date_obj, date_obj.strftime("%Y/%m/%d %H:%M:%S.%f %Z")

    def fmt_len(self, fmt):
        # deprecated: calculated length of string from 'struct' format specification
        # assumes on 's' data fromat
        return np.asarray(fmt[0:-1].split("s"), dtype="int").sum()

    def get_data(self, formatline=None):
        # reads data using the information in FORMAT
        # if FORMAT information in file header is missing or does not work
        # then create 'struct' data format based on channel details information
        idx = self.find_index("*END OF HEADER")
        lines = self.lines[idx + 1 :]
        data = []
        # if formatline is None, try reading without any format (assume columns are space limited;
        #       if space limited strategy does not work, try to create format line)
        if formatline is None:
            try:
                logger.debug(
                    "Trying to read file using format created using column width"
                )
                logger.debug(
                    "Reading data using format %s", self.channel_details["fmt_struct"]
                )
                fmt_len = self.fmt_len(self.channel_details["fmt_struct"])
                fmt_struct = self.channel_details["fmt_struct"]
                for i in range(len(lines)):
                    if len(lines[i].strip()) > 1:
                        # py2-3 migration...
                        # data.append(struct.unpack(self.channel_details['fmt_struct'], lines[i].rstrip().ljust(fmt_len)))
                        data.append(
                            struct.unpack(
                                fmt_struct,
                                lines[i].rstrip().ljust(fmt_len).encode("utf-8"),
                            )
                        )
                        # data.append([r for r in lines[i].split()])
            except Exception as e:
                data = np.genfromtxt(
                    StringIO("".join(lines)), delimiter="", dtype=str, comments=None
                )
                logger.info("Reading data using delimiter was successful !")

        else:
            ffline = ff.FortranRecordReader(formatline)
            for i in range(len(lines)):
                if len(lines[i]) > 0 and not re.match("\x1a+", lines[i]):
                    data.append([float(r) for r in ffline.read(lines[i])])
        data = np.asarray(data)
        logger.debug(data)
        # if data is at only one, convert list to 2D matrix
        if len(data.shape) == 1:
            data = data.reshape((1, -1))
        return data

    def get_location(self):
        # read 'LOCATION' section from ios header
        # convert lat and lon to standard format (float, -180 to +180)
        # initialize some other standard section variables if possible
        def _convert_latlong_string(ll):
            if not isinstance(ll, str):
                return ll
            # ignore trailing comments
            ll = ll.rsplit("!")[0]
            buf = ll.split()
            direction = -1 if len(buf) == 3 and buf[2] in ("S", "W") else 1
            return direction * (float(buf[0]) + float(buf[1]) / 60)

        info = self.get_section("LOCATION")
        logger.debug("Location details %s", info.keys())
        # Convert lat and lon
        info["LATITUDE"] = _convert_latlong_string(info.get("LATITUDE"))
        info["LONGITUDE"] = _convert_latlong_string(info.get("LONGITUDE"))
        # initialize some dict items if not available
        # if 'EVENT NUMBER' not in info.keys():
        # info['EVENT NUMBER'] = ''
        return info

    def get_channel_detail(self):
        # read channel details. create format_structure (fmt_struct)
        # based on channel details. This information may be used as backup if
        # file does not contain FORMAT specifier
        # tpp: modified to read all variables as strings.
        # This is done because 'Format' information in 'CHANNEL DETAIL'
        # is not a fortran compatible description
        # CHANGELOG July 2019: decipher python 'struct' format from channel details
        lines = self.get_subsection("TABLE: CHANNEL DETAIL", self.file)
        if lines is None:
            return None
        mask = lines[1].rstrip()
        info = {}
        ch_det = [self.apply_col_mask(l, mask) for l in lines[2:]]
        info["Pad"] = [l[1] for l in ch_det]
        info["Width"] = [l[3] for l in ch_det]
        info["Format"] = [l[4] for l in ch_det]
        info["Type"] = [l[5] for l in ch_det]
        if int(self.file["NUMBER OF CHANNELS"]) != len(info["Pad"]):
            raise Exception(
                "Number of channels in file record does not match channel_details!"
            )
        elif any([item for item in info["Type"] if item.strip()]) or any(
            [item for item in info["Format"] if item.strip()]
        ):
            fmt = ""
            for i in range(len(info["Pad"])):
                if info["Type"][i].strip() == "D":
                    fmt = fmt + "11s"
                elif info["Type"][i].strip() == "DT":
                    fmt = fmt + "17s"
                elif info["Format"][i].strip().upper() == "HH:MM:SS.SS":
                    fmt = fmt + "12s"
                elif info["Format"][i].strip().upper() == "HH:MM:SS":
                    fmt = fmt + "9s"
                elif info["Format"][i].strip().upper() == "HH:MM":
                    fmt = fmt + "6s"
                elif info["Width"][i].strip():
                    fmt = fmt + info["Width"][i].strip() + "s"
                elif re.match("F\d+\.\d+", info["Format"][i], re.IGNORECASE):
                    fmt = (
                        fmt
                        + re.match("F(\d+)\.\d+\s*", info["Format"][i], re.IGNORECASE)[
                            1
                        ]
                        + "s"
                    )
                elif re.match("I\d+", info["Format"][i], re.IGNORECASE):
                    fmt = (
                        fmt
                        + re.match("I(\d+)", info["Format"][i], re.IGNORECASE)[1]
                        + "s"
                    )
                elif info["Format"][i].strip() in ("F", "I", "f", "i"):
                    logger.info(
                        "Unable to retrieve the fmt format from the CHANNEL DETAIL Table"
                    )
                    break
                else:
                    logger.error(
                        "Unknown variable format Format: %s, Type: %s",
                        info["Format"][i],
                        info["Type"][i],
                    )
                    raise Exception(
                        "Unknown variable format Format: %s, Type: %s"
                        % (info["Format"][i], info["Type"][i])
                    )
            else:
                info["fmt_struct"] = fmt

            logger.debug("Python compatible data format: %s", fmt)
        return info

    def get_channels(self):
        # get the details of al the channels in the file
        # return as dictionary with each column as list
        lines = self.get_subsection("TABLE: CHANNELS", self.file)
        mask = lines[1].rstrip()
        info = {}
        ch = [self.apply_col_mask(l, mask) for l in lines[2:]]
        info["Name"] = [l[1] for l in ch]
        info["Units"] = [l[2] for l in ch]
        info["Minimum"] = [l[3] for l in ch]
        info["Maximum"] = [l[4] for l in ch]
        return info

    def apply_col_mask(self, data, mask):
        # apply mask to string (data) to get columns
        # return list of columns
        logger.debug("data=%s, mask=%s", data, mask)
        data = data.rstrip().ljust(len(mask))
        a = [d == "-" for d in mask]
        ret = []
        quoted = False
        pass_column_limit = False
        for i in range(len(data)):
            # Some IOS tables have quoted values that extend over the limits of the colmns
            if data[i] == "'":
                if quoted and pass_column_limit:
                    pass_column_limit = False
                    ret.append("*")
                quoted = not quoted
            elif not a[i] and not quoted:
                ret.append("*")
            elif not a[i]:
                pass_column_limit = True
            else:
                ret.append(data[i])
        buf = "".join(ret).split("*")
        while "" in buf:
            buf.remove("")
        return buf

    def get_comments_like(self, section_name):
        # to read sections like comments/remarks etc that are at 'root' level
        # and contain a lot of information that must be kept together
        # return information as a dictionary with identifier being line number
        if section_name[0] != "*":
            section_name = "*" + section_name.strip()
        idx = self.find_index(section_name)
        if idx == -1:
            return ""
        info = {}
        # EOS = False # end of section logical
        count = 0
        while True:
            idx += 1
            count += 1
            l = self.lines[idx]
            if len(l.strip()) == 0:  # skip line if blank
                continue
            elif l[0] == "!":
                continue
            elif l[0] in ["$", "*"]:
                break
            else:
                logger.debug(l)
                info["{:d}".format(count)] = l.rstrip()
        return info

    def get_list_of_sections(self):
        # parse the entire header and returns list of sections available
        # skip first 2 lines of file (that has date and ios_header_version)
        # skip * in beginning of section name
        sections_list = []
        for i, line in enumerate(self.lines[2:]):
            if (
                line[0] == "*"
                and line[0:4] != "*END"
                and line[1] not in ["*", " ", "\n"]
            ):
                sections_list.append(line.strip()[1:])
            else:
                continue
        logger.debug(sections_list)
        return sections_list

    def assign_geo_code(self, geojson_file):
        # read geojson file
        polygons_dict = read_geojson(geojson_file)
        geo_code = find_geographic_area(
            polygons_dict, Point(self.location["LONGITUDE"], self.location["LATITUDE"])
        )
        if geo_code == "":
            # geo_code = self.LOCATION['GEOGRAPHIC AREA'].strip()
            geo_code = "None"
        self.geo_code = geo_code

    def get_obs_time(self):
        # Return a timeseries
        from pandas import to_datetime

        chnList = [i.strip().lower() for i in self.channels["Name"]]

        if "time:utc" in chnList:
            chnList[chnList.index("time:utc")] = "time"

        if "date" in chnList and "time" in chnList:
            if isinstance(self.data[0, chnList.index("date")], bytes):
                dates = [
                    i.decode("utf8").strip()
                    for i in self.data[:, chnList.index("date")]
                ]
                times = [
                    i.decode("utf8").strip()
                    for i in self.data[:, chnList.index("time")]
                ]
            else:
                dates = [i.strip() for i in self.data[:, chnList.index("date")]]
                times = [i.strip() for i in self.data[:, chnList.index("time")]]
            datetime = to_datetime(
                [date.replace(" ", "") + " " + time for date, time in zip(dates, times)]
            )
            self.obs_time = datetime.to_pydatetime()
            self.obs_time = [
                timezone("UTC").localize(i + timedelta(hours=0)) for i in self.obs_time
            ]
        elif "date" in chnList:
            if isinstance(self.data[0, chnList.index("date")], bytes):
                dates = [
                    i.decode("utf8").strip()
                    for i in self.data[:, chnList.index("date")]
                ]
            else:
                dates = [i.strip() for i in self.data[:, chnList.index("date")]]
            datetime = to_datetime(dates)
            self.obs_time = datetime.to_pydatetime()
            self.obs_time = [
                timezone("UTC").localize(i + timedelta(hours=0)) for i in self.obs_time
            ]
        else:
            logger.info("Unable to find date/time columns in file")
            try:
                time_increment = self.get_dt()
                self.obs_time = [
                    self.start_dateobj + timedelta(seconds=time_increment * (i))
                    for i in range(int(self.file["NUMBER OF RECORDS"]))
                ]
            except Exception as e:
                raise Exception("ERROR: Unable to use time increment", self.filename)
                # return 0

        # date/time section in data is supposed to be in UTC.
        # check if they match, if not then raise fatal error
        dt = pd.Timedelta("1minute")
        if not (-dt < self.obs_time[0] - self.start_dateobj < dt):
            logger.error(
                "Error: First record in data does not match start date in header  self.obs_time[0]-self.start_dateobj=%s",
                self.obs_time[0] - self.start_dateobj,
            )
            return 0

    def add_ios_vocabulary(self, vocab=None):

        vocabulary_attributes = [
            "ios_name",
            "long_name",
            "standard_name",
            "units",
            "scale",
            "sdn_parameter_urn",
            "sdn_parameter_name",
            "sdn_uom_urn",
            "sdn_uom_name",
            "rename",
        ]

        def match_term(reference, value):
            if reference in (None, np.nan):
                return False
            if (
                ("None" in reference.split("|") and value in (None, "n/a", ""))
                or re.fullmatch(reference, value)
                or value in reference.split("|")
            ):
                return True
            return False

        # Load vocabulary
        if vocab is None or isinstance(vocab, str):
            vocab = read_ios_vocabulary(vocab)

        # Filter vocabulary to handle only file extension and global terms
        vocab = (
            vocab.query(
                f"ios_file_extension == '{self.get_file_extension()}' or ios_file_extension.isna()"
            )
            .sort_values("ios_file_extension")
            .set_index("ios_file_extension")
        )

        # iterate over variables and find matching vocabulary
        self.vocabulary_attributes = []
        for id, (name, units) in enumerate(
            zip(self.channels["Name"], self.channels["Units"])
        ):

            # Drop trailing spaces and commas
            name = re.sub(r"^\'|[\s\']+$", "", name.lower())
            units = re.sub(r"^\'|[\s\']+$", "", units)

            if re.match(r"\'*(flag|quality_flag)", name, re.IGNORECASE):
                self.vocabulary_attributes += [[self.get_flag_convention(name, units)]]
                continue
            if re.match("(Date|Time)", name, re.IGNORECASE):
                self.vocabulary_attributes += [[{}]]
                continue

            units = re.sub("^'|'$", "", units)
            name_match_type = vocab["ios_name"] == name.strip().lower()
            match_units = vocab["accepted_units"].apply(
                lambda x: match_term(x, units.strip())
            )

            matched_vocab = vocab.loc[name_match_type & match_units]
            if matched_vocab.empty:
                logger.warning(
                    "Missing vocabulary for file=%s; variable name=%s,units=%s",
                    self.filename,
                    name,
                    units,
                )
                self.vocabulary_attributes += [[{"long_name": name, "units": units}]]
                continue

            # consider only the vocabularies specific to this ios_file_extension group only
            matched_vocab = matched_vocab.filter(
                items=matched_vocab.index.get_level_values(0), axis="index"
            )
            self.vocabulary_attributes += [
                [
                    row.dropna().to_dict()
                    for _, row in matched_vocab[vocabulary_attributes].iterrows()
                ]
            ]

    def rename_duplicated_channels(self):
        old_channel_names = [chan.strip() for chan in self.channels["Name"]]
        new_channel_names = old_channel_names.copy()

        for id, chan in enumerate(old_channel_names):
            preceding_channels = old_channel_names[:id]
            if chan in preceding_channels:
                new_channel_names[
                    id
                ] = f"{chan}{len([c for c in preceding_channels if c==chan]) + 1:02g}"
                logger.info(
                    "Duplicated channel exists, rename channel %s to %s",
                    chan,
                    new_channel_names[id],
                )

        self.channels["Name"] = new_channel_names

    def rename_date_time_variables(self):
        rename_channels = self.channels["Name"]
        history = []
        for id, (chan, units) in enumerate(
            zip(self.channels["Name"], self.channels["Units"])
        ):
            if (
                chan.startswith("Time")
                and units.strip().lower() in ("days", "day_of_year")
            ) or chan.strip().lower() in ["time:day_of_year", "time:julian"]:
                rename_channels[id] = "Time:Day_of_Year"
            elif not re.search("^(time|date)", chan, re.IGNORECASE) or chan.strip() in (
                "Time",
                "Date",
            ):
                continue
            elif re.match(r"Date[\s\t]*($|YYYY/MM/DD)", chan.strip()):
                logger.warning("Rename variable '%s' -> 'Date'", chan)
                rename_channels[id] = "Date"
            elif re.match(r"Time[\s\t]*($|HH:MM:SS)", chan.strip()):
                logger.warning("Rename variable '%s' -> 'Time'", chan)
                history += [f"rename variable '{chan}' -> 'Time'"]
                rename_channels[id] = "Time"
            else:
                logger.warning(f"Unkown date time channel %s", chan)

        self.channels["Name"] = rename_channels

    def to_xarray(
        self,
        rename_variables=True,
        append_sub_variables=True,
        replace_date_time_variables=True,
    ):
        """Convert ios class to xarray dataset

        Returns:
            xarray dataset
        """

        def _format_attributes(section, prefix=""):
            def _format_attribute_name(name):
                if name == "$REMARKS":
                    return f"{section}_remarks"
                return f"{prefix}{name}".replace(" ", "_").lower()

            def _format_attribute_value(value):
                if isinstance(value, str):
                    return value.strip()
                elif isinstance(value, (float, int)):
                    return value
                elif isinstance(value, list):
                    return "".join(value)
                else:
                    return value

            attrs = getattr(self, section)
            if attrs:
                return {
                    _format_attribute_name(name): _format_attribute_value(value)
                    for name, value in attrs.items()
                    if value and not name.startswith("$TABLE:")
                }
            else:
                return {}

        def update_variable_index(varname, id):
            """Replace variable index (1,01,X,XX) by the given index or append
            0 padded index if no index exist in original variable name"""
            if varname.endswith(("01", "XX")):
                return f"{varname[:-2]}{id:02g}"
            elif varname.endswith(("1", "X")):
                return f"{varname[:-1]}{id}"
            return f"{varname}{id:02g}"

        def _drop_empty_attrs(attrs):
            if isinstance(attrs, dict):
                return {key: value for key, value in attrs.items() if value}
            return attrs

        # Fix time variable(s)
        self.rename_date_time_variables()

        # Retrieve the different variable attributes
        variables = (
            pd.DataFrame(
                {
                    "ios_name": self.channels["Name"],
                    "units": self.channels["Units"],
                    "ios_type": self.channel_details.get("Type")
                    if self.channel_details
                    else "",
                    "ios_format": self.channel_details.get("Format")
                    if self.channel_details
                    else "",
                    "pad": self.channel_details.get("Pad")
                    if self.channel_details
                    else "",
                }
            )
            .applymap(str.strip)
            .replace({"": None, "n/a": None})
        )
        variables["matching_vocabularies"] = self.vocabulary_attributes
        variables["dtype"] = (
            variables["ios_type"]
            .fillna(variables["ios_format"])
            .apply(get_dtype_from_ios_type)
        )
        if variables["dtype"].isna().any():
            variables["dtype"] = variables["dtype"].fillna(
                variables["ios_name"].apply(get_dtype_from_ios_name)
            )

        _FillValues = variables.apply(
            lambda x: pd.Series(x["pad"] or None).astype(x["dtype"]), axis="columns"
        )
        variables["_FillValues"] = _FillValues if not _FillValues.empty else None
        variables["renamed_name"] = variables.apply(
            lambda x: x["matching_vocabularies"][-1].get("rename", x["ios_name"]),
            axis="columns",
        )

        # Detect duplicated variables ios_name,units pairs
        duplicates = variables.duplicated(subset=["ios_name", "units"], keep=False)
        if duplicates.any():
            logger.warning(
                "Duplicated variables (Name,Units) pair was detected, only the first one will be considerd:\n%s",
                variables.loc[duplicates][["ios_name", "units"]],
            )
            variables.drop_duplicates(
                subset=["ios_name", "units"], keep="first", inplace=True
            )

        # Detect and rename duplicated variable names with different units
        col_name = "renamed_name" if rename_variables else "ios_name"
        duplicated_name = variables.duplicated(subset=[col_name])
        if duplicated_name.any():
            variables["var_index"] = variables.groupby(col_name).cumcount()
            to_replace = duplicated_name & (variables["var_index"] > 0)
            new_names = variables.loc[to_replace].apply(
                lambda x: update_variable_index(x[col_name], x["var_index"] + 1),
                axis="columns",
            )
            logger.warning(
                "Duplicated variable names, will rename the variables according to: %s",
                list(
                    zip(
                        variables.loc[
                            to_replace, set(["ios_name", "units"] + [col_name])
                        ]
                        .reset_index()
                        .values.tolist(),
                        "renamed -> " + new_names,
                    )
                ),
            )
            variables.loc[to_replace, col_name] = new_names

        # Parse data, assign appropriate data type, padding values
        #  and convert to xarray object
        ds = (
            pd.DataFrame.from_records(
                self.data[:, variables.index], columns=variables[col_name]
            )
            .replace("\.$", "", regex=True)
            .astype(dict(variables[[col_name, "dtype"]].values))
            .replace(
                dict(variables[[col_name, "_FillValues"]].dropna().values), value=np.nan
            )
            .to_xarray()
        )

        # Add variable attributes
        for id, row in variables.iterrows():
            var = ds[row[col_name]]
            vocab_attrs = row["matching_vocabularies"][-1]
            vocab_attrs.pop("rename", None)
            # Combine attributes and ignore empty values
            var.attrs = _drop_empty_attrs(
                {
                    "original_ios_variable": str(
                        {id: row[["ios_name", "units"]].to_json()}
                    ),
                    "original_ios_name": row["ios_name"],
                    "long_name": row["ios_name"],
                    "units": row["units"],
                    **vocab_attrs,
                }
            )

            if append_sub_variables:
                for new_var_attrs in row["matching_vocabularies"][:-1]:
                    if "rename" not in new_var_attrs:
                        continue
                    new_var = new_var_attrs.pop("rename")

                    # if variable already exist from a different source variable
                    #  append variable index
                    if new_var in ds:
                        if (
                            ds[new_var].attrs["original_ios_name"]
                            == var.attrs["original_ios_name"]
                        ):
                            logger.error("Duplicated vocabulary output for %s", row)
                            continue
                        else:
                            new_index = (
                                len([var for var in ds if var.startswith(new_var[:-1])])
                                + 1
                            )
                            logging.warning(
                                "Duplicated variable from sub variables: %s, rename +%s",
                                new_var,
                                new_index,
                            )
                            new_var = update_variable_index(new_var, new_index)

                    ds[new_var] = (var.dims, var.data, _drop_empty_attrs(new_var_attrs))

        # Replace date/time variables by a single time column
        if self.obs_time and replace_date_time_variables:
            ds = ds.drop([var for var in ds if var in ["Date", "Time"]])
            ds["time"] = (ds.dims, pd.Series(self.obs_time))
            ds["time"].encoding["units"] = "seconds since 1970-01-01T00:00:00Z"

        # Generate global attributes
        ds.attrs.update(_format_attributes("administration"))
        ds.attrs.update(
            {
                key: value
                for key, value in _format_attributes("file").items()
                if key not in ["format", "data_type", "file_type"]
            }
        )
        ds.attrs.update(_format_attributes("instrument", prefix="instrument_"))
        ds.attrs.update(_format_attributes("location"))
        if self.deployment:
            ds.attrs.update(_format_attributes("deployment", "deployment_"))
        if self.recovery:
            ds.attrs.update(_format_attributes("recovery", "recovery_"))
        if self.comments:
            ds.attrs["comments"] = str(self.comments)
        if self.remarks:
            ds.attrs["remarks"] = str(self.remarks)
        if self.history:
            ds.attrs["history"] = str(self.history)
        if hasattr(self, "geo_code"):
            ds.attrs["geographical_area"] = self.geo_code
        ds.attrs["comments"] = ds.attrs.pop("file_remarks", None)
        ds.attrs["header"] = json.dumps(
            self.get_complete_header(), ensure_ascii=False, indent=False
        )
        ds.attrs["start_time"] = self.start_dateobj.isoformat()
        ds.attrs["end_time"] = (
            self.end_dateobj.isoformat() if self.end_dateobj else None
        )
        ds.attrs["source"] = self.filename
        ds.attrs["ios_header_version"] = self.ios_header_version
        ds.attrs["cioos_data_transform_version"] = VERSION
        ds.attrs[
            "product_version"
        ] = f"ios_header={self.ios_header_version}; cioos_transform={VERSION}"
        ds.attrs["date_created"] = self.date_created.isoformat()

        # Geospatial attributes
        ds.attrs["start_time"] = self.start_dateobj.isoformat()
        ds.attrs["end_time"] = (
            self.end_dateobj.isoformat() if self.end_dateobj else None
        )
        ds.attrs["time_coverage_start"] = ds.attrs["start_time"]
        ds.attrs["time_coverage_end"] = (
            ds.attrs.get("end_time") or ds.attrs["start_time"]
        )
        ds.attrs["time_coverage_duration"] = pd.Timedelta(
            (self.end_dateobj or self.start_dateobj) - self.start_dateobj
        ).isoformat()
        ds.attrs["time_coverage_resolution"] = (
            pd.Timedelta(self.time_increment).isoformat()
            if self.time_increment
            else None
        )

        ds.attrs["geospatial_lat_min"] = (
            ds["latitude"].min().item(0) if "latitude" in ds else ds.attrs["latitude"]
        )
        ds.attrs["geospatial_lat_max"] = (
            ds["latitude"].max().item(0) if "latitude" in ds else ds.attrs["latitude"]
        )
        ds.attrs["geospatial_lat_units"] = "degrees_north"
        ds.attrs["geospatial_lon_min"] = (
            ds["longitude"].min().item(0)
            if "longitude" in ds
            else ds.attrs["longitude"]
        )
        ds.attrs["geospatial_lon_max"] = (
            ds["longitude"].max().item(0)
            if "longitude" in ds
            else ds.attrs["longitude"]
        )
        ds.attrs["geospatial_lon_units"] = "degrees_east"

        if "depth" in ds:
            ds.attrs["geospatial_vertical_min"] = ds["depth"].min().item(0)
            ds.attrs["geospatial_vertical_max"] = ds["depth"].max().item(0)
            ds.attrs["geospatial_vertical_positive"] = "down"
            ds.attrs["geospatial_vertical_units"] = ds["depth"].attrs["units"]

        # replace index dimension by the appropriate dimension for this file
        if "time" in ds and ds["index"].size == ds["time"].size:
            ds = ds.swap_dims({"index": "time"})
        elif "depth" in ds and ds["index"].size == ds["depth"].size:
            ds = ds.swap_dims({"index": "depth"})

        # Define featureType attribute
        if "time" in ds.dims:
            if (
                "latitude" in ds
                and "time" in ds["latitude"].dims
                and "longitude" in ds
                and "time" in ds["longitude"].dims
            ):
                featureType = "trajectory"
            else:
                featureType = "timeSeries"
        else:
            featureType = ""
        if "depth" in ds.dims:
            featureType += "Profile" if featureType else "profile"
        ds.attrs["featureType"] = featureType
        ds.attrs["cdm_data_type"] = featureType.title()

        # Set coordinate variables
        coordinates_variables = ["time", "latitude", "longitude", "depth"]
        if any(var in ds for var in coordinates_variables):
            ds = ds.set_coords([var for var in coordinates_variables if var in ds])
            if "index" in ds.coords and ("time" in ds.coords or "depth" in ds.coords):
                ds = ds.reset_coords("index").drop("index")

        # Drop empty attributes and variable attribtes
        ds.attrs = {key: value for key, value in ds.attrs.items() if value}
        for var in ds:
            ds[var].attrs = {
                key: value for key, value in ds[var].attrs.items() if value
            }
        return ds


#
# ********************              END DEFINITION FOR OBSFILE CLASS          **************************
#


class CtdFile(ObsFile):
    """
    Read CTD file in IOS format
    inherits methods from ObsFile class creates a new method called import_data
    this method processes files in manner that is specific to CTD dataset
    Author: Pramod Thupaki pramod.thupaki@hakai.org
    """

    def import_data(self):
        self.type = "ctd"
        self.start_dateobj, self.start_date = self.get_date(opt="start")
        self.location = self.get_location()
        self.channels = self.get_channels()
        self.comments = self.get_comments_like("COMMENTS")
        self.remarks = self.get_comments_like("REMARKS")
        self.administration = self.get_section("ADMINISTRATION")
        self.instrument = self.get_section("INSTRUMENT")
        self.channel_details = self.get_channel_detail()
        if self.channel_details is None:
            logger.info("Unable to get channel details from header...")

        # try reading file using format specified in 'FORMAT'
        try:
            self.data = self.get_data(formatline=self.file.get("FORMAT"))
        except Exception as e:
            self.data = None
            logger.info("Could not read file using 'FORMAT' description ...")
        if self.data is None:
            try:
                # self.channel_details = self.get_channel_detail()
                self.data = self.get_data(formatline=None)
            except Exception as e:
                logger.error(
                    "Faield to parse data with and without 'FORMAT' description"
                )
                return 0

        return 1


class CurFile(ObsFile):
    """
    Read current meter file in IOS format
    """

    def import_data(self):
        self.type = "cur"
        self.start_dateobj, self.start_date = self.get_date(opt="start")
        self.location = self.get_location()
        self.channels = self.get_channels()
        self.comments = self.get_comments_like("COMMENTS")
        self.remarks = self.get_comments_like("REMARKS")
        self.administration = self.get_section("ADMINISTRATION")
        self.instrument = self.get_section("INSTRUMENT")
        self.deployment = self.get_section("DEPLOYMENT")
        self.recovery = self.get_section("RECOVERY")
        # time_increment = self.get_dt()

        self.channel_details = self.get_channel_detail()
        if self.channel_details is None:
            logger.info("Unable to get channel details from header...")
        # try reading file using format specified in 'FORMAT'
        try:
            self.data = self.get_data(formatline=self.file["FORMAT"])
        except Exception as e:
            logger.info("Could not read file using 'FORMAT' description...")
            self.data = None

        if self.data is None:
            try:
                # self.channel_details = self.get_channel_detail()
                self.data = self.get_data(formatline=None)
            except Exception as e:
                logger.error(
                    "Could not read file using 'struct' data format description..."
                )
                return 0

        # if time_increment is None:
        # logger.info("Did not find 'TIME INCREMENT'. Trying to calculate it from endtime and nrecs ...")
        # enddateobj, _ = self.get_date(opt='end')
        # logger.info((enddateobj - self.start_dateobj).total_seconds(), self.file['NUMBER OF RECORDS'])
        # time_increment = (enddateobj - self.start_dateobj).total_seconds()/(int(self.file['NUMBER OF RECORDS'])-1)
        # logger.info('New time increment =', time_increment)
        # logger.info('Getting time increment from data section ...')

        # get timeseries times from data directly. raise fatal error if not availale
        # (2021/Jan - tpp) time increment based method is nor reliable (burst mode/ incorrect nrec etc.)
        self.get_obs_time()
        # # Take difference of first two times in self.data
        # time_increment = (self.obs_time[1] - self.obs_time[0]).total_seconds()

        # self.obs_time = [self.start_dateobj + timedelta(seconds=time_increment * (i))
        # for i in range(int(self.file['NUMBER OF RECORDS']))]

        return 1


class MCtdFile(ObsFile):
    """
    Read Mooring CTD file in IOS format
    inherits methods from ObsFile class creates a new method called import_data
    this method processes files in manner that is specific to CTD dataset
    Author: Pramod Thupaki pramod.thupaki@hakai.org
    """

    def import_data(self):
        from datetime import timedelta

        self.type = "mctd"
        self.start_dateobj, self.start_date = self.get_date(opt="start")
        self.location = self.get_location()
        self.channels = self.get_channels()
        self.comments = self.get_comments_like("COMMENTS")
        self.remarks = self.get_comments_like("REMARKS")
        self.administration = self.get_section("ADMINISTRATION")
        self.instrument = self.get_section("INSTRUMENT")
        self.deployment = self.get_section("DEPLOYMENT")
        self.recovery = self.get_section("RECOVERY")
        time_increment = self.get_dt()
        self.channel_details = self.get_channel_detail()
        if self.channel_details is None:
            logger.info("Unable to get channel details from header...")

        # if time_increment is None:
        # logger.info("Did not find 'TIME INCREMENT'. Trying to calculate it from endtime and nrecs ...")
        # enddateobj, _ = self.get_date(opt='end')
        # time_increment = (enddateobj - self.start_dateobj).total_seconds()/(int(self.file['NUMBER OF RECORDS'])-1)
        # logger.info('New time increment =', time_increment)

        # self.obs_time = [self.start_dateobj + timedelta(seconds=time_increment * (i))
        #  for i in range(int(self.file['NUMBER OF RECORDS']))]
        logger.debug("first obs_time[[0,1]]=%s", [self.obs_time[0], self.obs_time[-1]])
        # try reading file using format specified in 'FORMAT'
        try:
            self.data = self.get_data(formatline=self.file["FORMAT"])
        except Exception as e:
            logger.error("Could not read file using 'FORMAT' description...")
            self.data = None

        if self.data is None:
            try:
                # self.channel_details = self.get_channel_detail()
                self.data = self.get_data(formatline=None)
            except Exception as e:
                return 0

        # get timeseries times from data directly. raise fatal error if not availale
        # (2021/Jan - tpp) time increment based method is nor reliable (burst mode/ incorrect nrec etc.)

        self.get_obs_time()

        return 1


class BotFile(ObsFile):
    """
    Read bottle files in IOS format
    """

    def import_data(self):
        self.type = "bot"
        self.start_dateobj, self.start_date = self.get_date(opt="start")
        self.location = self.get_location()
        self.channels = self.get_channels()
        self.comments = self.get_comments_like("COMMENTS")
        self.remarks = self.get_comments_like("REMARKS")
        self.administration = self.get_section("ADMINISTRATION")
        self.instrument = self.get_section("INSTRUMENT")
        self.channel_details = self.get_channel_detail()
        if self.channel_details is None:
            logger.warning("Unable to get channel details from header...")
        # try reading file using format specified in 'FORMAT'
        try:
            self.data = self.get_data(formatline=self.file["FORMAT"])
        except Exception as e:
            logger.error("Could not read file using 'FORMAT' description...")
            self.data = None

        if self.data is None:
            try:
                # self.channel_details = self.get_channel_detail()
                self.data = self.get_data(formatline=None)
            except Exception as e:
                return 0

        return 1


class GenFile(ObsFile):
    """General method used to parse the different IOS data types."""

    def import_data(self):
        sections_available = self.get_list_of_sections()
        self.type = None
        self.start_dateobj, self.start_date = self.get_date(opt="start")
        self.end_dateobj, self.end_date = (
            self.get_date(opt="end") if "END TIME" in self.file else (None, None)
        )
        self.time_increment = self.get_dt() if "TIME INCREMENT" in self.file else None
        self.location = self.get_location()
        self.channels = self.get_channels()
        self.comments = self.get_comments_like("COMMENTS")
        self.remarks = self.get_comments_like("REMARKS")
        self.administration = self.get_section("ADMINISTRATION")
        self.instrument = self.get_section("INSTRUMENT")
        self.channel_details = self.get_channel_detail()
        self.history = self.get_section("HISTORY")
        if "DEPLOYMENT" in sections_available:
            self.deployment = self.get_section("DEPLOYMENT")
        if "RECOVERY" in sections_available:
            self.recovery = self.get_section("RECOVERY")

        if self.channel_details is None:
            logger.info("Unable to get channel details from header...")
        # try reading file using format specified in 'FORMAT'
        try:
            self.data = self.get_data(formatline=self.file.get("FORMAT"))
        except Exception as e:
            logger.info(
                "Could not read file data using FORMAT=%s ",
                self.file.get("FORMAT"),
            )
            self.data = None

        if self.data is None:
            try:
                # self.channel_details = self.get_channel_detail()
                self.data = self.get_data(formatline=None)
            except Exception as e:
                logger.error("Failed to read file: %s", self.filename)
                return 0

        chnList = [i.strip().lower() for i in self.channels["Name"]]
        if "date" in chnList and ("time" in chnList or "time:utc" in chnList):
            self.get_obs_time()
        return 1
