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
from pytz import timezone
from .utils.utils import find_geographic_area, read_geojson, read_ios_vocabulary
from shapely.geometry import Point
import pandas as pd
from io import StringIO

ios_dtypes_to_python = {
    "R": "float32",
    "F": "float32",
    "I": "int32",
    "D": str,
    "T": str,
    "E": "float32",
}


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
            self.file = self.get_section("FILE")
            self.status = 1
        except Exception as e:
            print("Unable to open file", filename)
            print(e)
            self.status = 0
            exit(0)

    def import_data(self):
        pass

    def get_header_version(self):
        # reads header version
        return self.lines[self.find_index("*IOS HEADER VERSION")][20:24]

    def find_index(self, string):
        # finds line number that starts with string
        # input: string (nominally the section)
        for i, l in enumerate(self.lines):
            if l.lstrip()[0 : len(string)] == string:
                return i
        if self.debug:
            print("Index not found", string)
        return -1

    def get_complete_header(self):
        # return all sections in header as a dict
        sections = self.get_list_of_sections()
        header = {}
        for sec in sections:
            # print ("getting section:", sec)
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
            print("Section not found" + section_name + self.filename)
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
                if self.debug:
                    print(
                        "Found subsection:{} in section:{}".format(
                            record_name, section_name
                        )
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
                if self.debug:
                    print(l)
                if len(l.split(":", 1)) > 1:
                    info[l.split(":", 1)[0].strip()] = l.split(":", 1)[1]
        return info

    def get_subsection(self, name, section):
        # return subsection information from a section
        # used as interface for data actually read into dictionary by get_section
        # provides some useful debug information
        # returns lines that make up 'subsection' if all is well
        info = None
        if name[0] != "$":
            if self.debug:
                print("Finding subsection", name)
            name = "$" + name
        if name not in self.file.keys():
            print("Did not find subsection:{} in {}".format(name, self.filename))
        elif name == "$TABLE: CHANNELS":
            info = self.file[name]
        elif name == "$TABLE: CHANNEL DETAIL":
            info = self.file[name]
        return info

    def get_dt(self):
        # converts time increment from ios format to seconds
        # float32 accurate (seconds are not rounded to integers)
        if "TIME INCREMENT" in self.file:
            line = self.file["TIME INCREMENT"]
            dt = np.asarray(line.split("!")[0].split(), dtype=float)
            dt = sum(dt * [24.0 * 3600.0, 3600.0, 60.0, 1.0, 0.001])  # in seconds
        else:
            print("Time Increment not found in Section:FILE", self.filename)
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
        if self.debug:
            print("Raw date string:", date_string)
        # get the naive (timezone unaware) datetime obj
        try:
            date_obj = datetime.strptime(date_string[4:], "%Y/%m/%d %H:%M:%S.%f")
        except Exception as e:
            print(e)
            date_obj = datetime.strptime(date_string[4:], "%Y/%m/%d")
            print(date_obj)
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
        if self.debug:
            print("Date obj with timezone info:", date_obj)
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
                print("Trying to read file using format created using column width")
                print("Reading data using format", self.channel_details["fmt_struct"])
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
                print("Reading data using delimiter was successful !")

        else:
            ffline = ff.FortranRecordReader(formatline)
            for i in range(len(lines)):
                if len(lines[i]) > 0:
                    data.append([float(r) for r in ffline.read(lines[i])])
        data = np.asarray(data)
        if self.debug:
            print(data)
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
            buf = ll.split()
            direction = -1 if len(buf) == 3 and buf[2] in ("S", "W") else 1
            return direction * (float(buf[0]) + float(buf[1]) / 60)

        info = self.get_section("LOCATION")
        if self.debug:
            print("Location details", info.keys())
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
        else:
            fmt = ""
            for i in range(len(info["Pad"])):
                if info["Type"][i].strip() == "D":
                    fmt = fmt + "11s"
                elif info["Type"][i].strip() == "DT":
                    fmt = fmt + "17s"
                elif info["Format"][i].strip().upper() == "HH:MM:SS":
                    fmt = fmt + "9s"
                elif info["Format"][i].strip().upper() == "HH:MM":
                    fmt = fmt + "6s"
                else:
                    fmt = fmt + info["Width"][i].strip() + "s"

            info["fmt_struct"] = fmt
        if self.debug:
            print("Python compatible data format:", fmt)
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
        if self.debug:
            print(data, mask)
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
                if self.debug:
                    print(l)
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
        if self.debug:
            print(sections_list)
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
                [date + " " + time for date, time in zip(dates, times)]
            )
            self.obs_time = datetime.to_pydatetime()
            self.obs_time = [
                timezone("UTC").localize(i + timedelta(hours=0)) for i in self.obs_time
            ]
        elif "date" in chnList and "time" not in chnList:
            if isinstance(self.data[0, chnList.index("date")], bytes):
                dates = [
                    i.decode("utf8").strip()
                    for i in self.data[:, chnList.index("date")]
                ]
            else:
                dates = [i.strip() for i in self.data[:, chnList.index("date")]]
            datetime = to_datetime([date for date in dates])
            self.obs_time = datetime.to_pydatetime()
            self.obs_time = [
                timezone("UTC").localize(i + timedelta(hours=0)) for i in self.obs_time
            ]
        else:
            print("Unable to find date/time columns in file", self.filename)
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

        if self.obs_time[0] != self.start_dateobj:
            print(self.obs_time[0], self.start_dateobj)
            print(
                "Error: First record in data does not match start date in header",
                self.filename,
            )
            return 0

    def add_ios_vocabulary(self, vocab=None):
        def match_term(reference, value):
            if (
                reference in (None, np.nan, "None")
                or re.search(reference, value)
                or reference in value
            ):
                return True
            return False

        def _generate_vocabulary_attr():
            return [
                {attr: value for (attr, value) in row.to_dict().items() if value}
                for id, row in matched_vocab.iterrows()
            ]

        def _add_to_missing_vocabulary(name, units, data_type="float32"):
            print(f"Missing vocabulary for name: {name} Units: {units}")
            with open("missing_vocabulary.txt", "a") as handle:
                handle.write(
                    f"{self.filename},{name.lower()},{data_type},{re.sub('[:_]',' ',name)},,,{units},{units}\n"
                )

        def _save_variable(name, units, data_type="float32"):
            with open("variable.log", "a") as handle:
                handle.write(f"{self.filename},{name.lower()},{data_type},{units}\n")

        # Load vocabulary
        if vocab is None or isinstance(vocab, str):
            vocab = read_ios_vocabulary(vocab)

        # iterate over variables and find matching vocabulary
        self.vocabulary_attributes = {}
        for id, (name, units) in enumerate(
            zip(self.channels["Name"], self.channels["Units"])
        ):

            # Drop trailing spaces and commas
            name = re.sub(r"^\'|[\s\']+$", "", name)
            units = re.sub(r"^\'|[\s\']+$", "", units)

            if re.match(r"\'*(flag|quality_flag)", name, re.IGNORECASE):
                # TODO add flag related metadata
                self.vocabulary_attributes[name] = [{}]
                continue
            elif re.match("(Date|Time)", name, re.IGNORECASE):
                self.vocabulary_attributes[name] = [{}]
                continue

            units = re.sub("^'|'$", "", units)
            name_match_type = vocab["name"] == name.strip().lower()
            match_units = vocab["accepted_units"].apply(
                lambda x: match_term(x, units.strip())
            )

            matched_vocab = vocab.loc[name_match_type & match_units]
            data_type = (
                self.channel_details["Format"][id].strip()
                if self.channel_details
                else None
            )
            _save_variable(name.strip(), units.strip(), data_type)
            if not matched_vocab.empty:
                self.vocabulary_attributes[name] = _generate_vocabulary_attr()
            else:
                _add_to_missing_vocabulary(name.strip(), units.strip(), data_type)

    def get_channel_attributes(self):
        def _map_dtype(ios_type):
            if ios_type.strip() in (None,""):
                return str
            elif ios_type in ios_dtypes_to_python:
                return ios_dtypes_to_python[ios_type]
            elif ios_type[0] in ios_dtypes_to_python:
                return ios_dtypes_to_python[ios_type[0]]

        if self.channel_details is None:
            print("Channel details not available")
            return {}

        channel_attributes = (
            pd.DataFrame({**self.channels, **self.channel_details})
            .set_index("Name")
            .applymap(lambda x: x.strip())
            .drop(columns=["fmt_struct"])
        )

        # Generate dtype attribute r
        channel_attributes["dtype"] = (
            channel_attributes["Type"]
            .apply(_map_dtype)
            .fillna(channel_attributes["Format"].apply(_map_dtype))
        )

        # Detect missing mapping
        is_missing_dtype = channel_attributes["dtype"].isna()
        if is_missing_dtype.any():
            missing_dtype_mapping_str = channel_attributes.loc[is_missing_dtype][
                ["Format", "Type"]
            ].to_json(orient="index")
            print(f"Missing dtype mapping for {missing_dtype_mapping_str}")
            channel_attributes["dtype"].fillna("str", inplace=True)
        return channel_attributes.to_dict(orient="index")

    def rename_duplicated_channels(self):
        old_channel_names = [chan.strip() for chan in self.channels["Name"]]
        new_channel_names = old_channel_names.copy()

        for id, chan in enumerate(old_channel_names):
            preceding_channels = old_channel_names[:id]
            if chan in preceding_channels:
                new_channel_names[
                    id
                ] = f"{chan}{len([c for c in preceding_channels if c==chan]) + 1:02g}"

        self.channels["Name"] = new_channel_names

    def rename_date_time_variables(self):
        rename_channels = self.channels["Name"]
        for id, chan in enumerate(self.channels["Name"]):
            if not re.search("^(time|date)", chan, re.IGNORECASE):
                continue
            elif re.match(r"Date[\s\t]*($|YYYY/MM/DD)", chan.strip()):
                rename_channels[id] = "Date"
            elif re.match(r"Time[\s\t]*($|HH:MM:SS)", chan.strip()):
                rename_channels[id] = "Time"
            else:
                print(f"Unkown date time channel {chan}")

    def to_xarray(self, rename_variables=True):
        """Convert ios class to xarray dataset

        Returns:
            xarray dataset
        """

        def make_variable_names_compatiple(varname):
            varname = re.sub(r"[-\.\[\]\s\:\/\\\'\"]", "_", varname)
            varname = re.sub("%", "perc", varname)
            varname = re.sub(r"_+", "_", varname)
            varname = re.sub(r"^_|_$", "", varname)
            return varname

        def _format_attributes(attrs, prefix=""):
            return {
                f"{prefix}{name}".replace(" ", "_").lower(): value.strip()
                if isinstance(value, str)
                else value
                for name, value in attrs.items()
                if isinstance(value, (float, int, str))
            }

        # Fix some issues
        self.rename_duplicated_channels()
        self.rename_date_time_variables()

        # Parse data
        df = pd.DataFrame.from_records(self.data, columns=self.channels["Name"])
        # Format data type
        # TODO replace Pad values
        channel_attributes = self.get_channel_attributes()
        df = df.astype(
            {chan: attrs["dtype"] for chan, attrs in channel_attributes.items()}
        )
        ds = df.to_xarray()

        # Generate global attributes
        ds.attrs.update(_format_attributes(self.administration))
        ds.attrs.update(_format_attributes(self.file))
        ds.attrs.update(_format_attributes(self.instrument, prefix="instrument_"))
        ds.attrs.update(_format_attributes(self.location))
        ds.attrs["comments"] = str(self.comments)
        ds.attrs["remarks"] = str(self.remarks)
        if self.deployment:
            ds.attrs.update(_format_attributes(self.recovery, "deployment_"))
        if self.recovery:
            ds.attrs.update(_format_attributes(self.recovery, "recovery_"))

        # Generate Variable attributes
        ios_variables_attributes = (
            pd.DataFrame(
                {
                    **self.channels,
                    **(self.channel_details if self.channel_details else {}),
                }
            )
            .apply(lambda x: x.strip() if isinstance(x, str) else x)
            .set_index(["Name"])
            .drop(columns=["fmt_struct"], errors="ignore")
            .to_dict(orient="index")
        )
        for var, attrs in ios_variables_attributes.items():
            ds[var.strip()].attrs.update(
                {
                    **{attr.lower(): value for attr, value in attrs.items()},
                    **(
                        self.vocabulary_attributes[var][0]
                        if var in self.vocabulary_attributes
                        else {}
                    ),
                }
            )

        # Convert any object variables to strings
        for var in ds:
            if ds[var].dtype == object:
                ds[var] = ds[var].astype(str)

        if rename_variables:
            ds = ds.rename({var: make_variable_names_compatiple(var) for var in ds})

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
            print("Unable to get channel details from header...")

        # try reading file using format specified in 'FORMAT'
        try:
            self.data = self.get_data(formatline=self.file["FORMAT"])
        except Exception as e:
            self.data = None
            print("Could not read file using 'FORMAT' description ...", self.filename)
        if self.data is None:
            try:
                # self.channel_details = self.get_channel_detail()
                self.data = self.get_data(formatline=None)
            except Exception as e:
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
            print("Unable to get channel details from header...")
        # try reading file using format specified in 'FORMAT'
        try:
            self.data = self.get_data(formatline=self.file["FORMAT"])
        except Exception as e:
            print("Could not read file using 'FORMAT' description...")
            self.data = None

        if self.data is None:
            try:
                # self.channel_details = self.get_channel_detail()
                self.data = self.get_data(formatline=None)
            except Exception as e:
                print("Could not read file using 'struct' data format description...")
                return 0

        # if time_increment is None:
        # print("Did not find 'TIME INCREMENT'. Trying to calculate it from endtime and nrecs ...")
        # enddateobj, _ = self.get_date(opt='end')
        # print((enddateobj - self.start_dateobj).total_seconds(), self.file['NUMBER OF RECORDS'])
        # time_increment = (enddateobj - self.start_dateobj).total_seconds()/(int(self.file['NUMBER OF RECORDS'])-1)
        # print('New time increment =', time_increment)
        # print('Getting time increment from data section ...')

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
            print("Unable to get channel details from header...")

        # if time_increment is None:
        # print("Did not find 'TIME INCREMENT'. Trying to calculate it from endtime and nrecs ...")
        # enddateobj, _ = self.get_date(opt='end')
        # time_increment = (enddateobj - self.start_dateobj).total_seconds()/(int(self.file['NUMBER OF RECORDS'])-1)
        # print('New time increment =', time_increment)

        # self.obs_time = [self.start_dateobj + timedelta(seconds=time_increment * (i))
        #  for i in range(int(self.file['NUMBER OF RECORDS']))]
        if self.debug:
            print(self.obs_time[0], self.obs_time[-1])
        # try reading file using format specified in 'FORMAT'
        try:
            self.data = self.get_data(formatline=self.file["FORMAT"])
        except Exception as e:
            print("Could not read file using 'FORMAT' description...")
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
            print("Unable to get channel details from header...")
        # try reading file using format specified in 'FORMAT'
        try:
            self.data = self.get_data(formatline=self.file["FORMAT"])
        except Exception as e:
            print("Could not read file using 'FORMAT' description...")
            self.data = None

        if self.data is None:
            try:
                # self.channel_details = self.get_channel_detail()
                self.data = self.get_data(formatline=None)
            except Exception as e:
                return 0

        return 1


class GenFile(ObsFile):
    def import_data(self):
        self.type = None
        self.start_dateobj, self.start_date = self.get_date(opt="start")
        self.location = self.get_location()
        self.channels = self.get_channels()
        self.comments = self.get_comments_like("COMMENTS")
        self.remarks = self.get_comments_like("REMARKS")
        self.administration = self.get_section("ADMINISTRATION")
        self.instrument = self.get_section("INSTRUMENT")
        self.channel_details = self.get_channel_detail()
        if "DEPLOYMENT" in self.file:
            self.deployment = self.get_section("DEPLOYMENT")
        if "RECOVERY" in self.file:
            self.recovery = self.get_section("RECOVERY")

        if self.channel_details is None:
            print("Unable to get channel details from header...")
        # try reading file using format specified in 'FORMAT'
        try:
            self.data = self.get_data(formatline=self.file.get("FORMAT"))
        except Exception as e:
            print("Could not read file using 'FORMAT' description...")
            self.data = None

        if self.data is None:
            try:
                # self.channel_details = self.get_channel_detail()
                self.data = self.get_data(formatline=None)
            except Exception as e:
                return 0

        return 1
