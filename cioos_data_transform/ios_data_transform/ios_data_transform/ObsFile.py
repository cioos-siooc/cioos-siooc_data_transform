"""
    Python class to read IOS data files and store data for conversion to netcdf format
    Changelog Version 0.1: July 15 2019 - convert python scripts and functions into a python class
    Author: Pramod Thupaki (pramod.thupaki@hakai.org)
"""
import sys
class ObsFile(object):
    """
    Class template for all observed data.
    Contains data from the IOS file and methods to read the IOS format
    Specific improvements/modifications required to read filetypes will be make in derived classes
    Author: Pramod Thupaki pramod.thupaki@hakai.org
    Incorporates functions from earlier versions of this toolbox
    """
    def __init__(self, filename, debug):
        # initializes object by reading *FILE and ios_header_version
        # reads entire file to memory for all subsequent processing
        # inputs are filename and debug state
        self.debug = debug
        self.filename = filename
        with open(self.filename, 'r', encoding='ASCII', errors='ignore') as fid:
            self.lines = [l for l in fid.readlines()]
        self.ios_header_version = self.get_header_version()
        self.FILE = self.get_section('FILE')

    def get_header_version(self):
        # reads header version
        return self.lines[self.find_index('*IOS HEADER VERSION')][20:24]

    def find_index(self, string):
        # finds linenumber that starts with string
        # input: string (nominally the section)
        for i, l in enumerate(self.lines):
            if l.lstrip()[0:len(string)] == string:
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
            if sec in ['COMMENTS', 'REMARKS', 'HISTORY']:
                header[sec] = self.get_comments_like(sec)
            else:
                header[sec] = self.get_section(sec)
        return header

    def get_section(self, section_name):
        # deciphers the information in a particular section
        # reads table information
        # returns information as dictionary.
        # records (subsections) are returned as list of lines for subsequent processing
        if section_name[0] != '*':
            section_name = '*' + section_name
        idx = self.find_index(section_name)
        if idx == -1:
            print('Section not found'+section_name+self.filename)
            return {}
        info = {}
        # EOS = False # end of section logical
        while True:
            idx += 1
            l = self.lines[idx]
            if len(l.strip()) == 0: # skip line if blank
                continue
            elif l[0] == '!':
                continue
            elif l[0] in ['$', '*']:
                break
            elif '$' in l[1:5]:
                # read record or 'sub-section'. This nominally starts with tab of 4 spaces
                # but can be 1 or 2 spaces as well for REMARKS
                EOR = False
                record_name = l.strip()
                if self.debug:
                    print("Found subsection:{} in section:{}".format(record_name, section_name))
                info[record_name] = []
                while not EOR:
                    idx += 1
                    l = self.lines[idx]
                    if l.strip()[0:4] == '$END':
                        EOR = True
                    else:
                        info[record_name].append(l)
            else:
                if self.debug:
                    print(l)
                if len(l.split(':', 1)) > 1:
                    info[l.split(':', 1)[0].strip()] = l.split(':', 1)[1]
        return info

    def get_subsection(self, name, section):
        # return subsection information from a section
        # used as interface for data actually read into dictionary by get_section
        # provides some useful debug information
        # returns lines that make up 'subsection' if all is well
        if name[0] != '$':
            if self.debug:
                print("Finding subsection", name)
            name = '$' + name
        if name == '$TABLE: CHANNELS':
            info = self.FILE[name]
        elif name == '$TABLE: CHANNEL DETAIL':
            info = self.FILE[name]
        else:
            print(self.FILE.keys())
            raise Exception("Did not find subsection:{} in {}".format(name, self.filename))
        return info

    def get_dt(self):
        # converts time increment from ios format to seconds
        # float32 accurate (seconds are not rounded to integers)
        import numpy as np
        line = self.FILE['TIME INCREMENT']
        dt = np.asarray(line.split('!')[0].split(), dtype=float)
        dt = sum(dt*[24.*3600., 3600., 60., 1., 0.001])  # in seconds
        return dt

    def get_date(self, opt='start'):
        # reads datetime string in "START TIME" and converts to datetime object
        # return datetime object and as standard string format
        # read 'END TIME' if opt is 'end'
        from datetime import datetime
        from pytz import timezone

        if opt.lower() == 'start':
            date_string = self.FILE['START TIME'].strip().upper()
        elif opt.lower() == 'end':
            date_string = self.FILE['END TIME'].strip().upper()
        else:
            raise Exception("Invalid option for get_date function !")
        if self.debug:
            print("Raw date string:", date_string)
# get the naive (timezone unaware) datetime obj
        date_obj = datetime.strptime(date_string[4:], '%Y/%m/%d %H:%M:%S.%f')
# make datetime object, aware of its timezone
# for GMT, UTC
        if any([date_string.find(z) == 0 for z in ['GMT', 'UTC']]):
            date_obj = timezone(date_string[0:3]).localize(date_obj)
# for Canada/Pacific
        elif any([date_string.find(z) == 0 for z in ['PDT', 'PST']]):
            date_obj = timezone('Canada/Pacific').localize(date_obj)
# Canada/Mountain
        elif any([date_string.find(z) == 0 for z in ['MDT', 'MST']]):
            date_obj = timezone('Canada/Mountain').localize(date_obj)
# Canada/Atlantic
        elif any([date_string.find(z) == 0 for z in ['ADT', 'AST']]):
            date_obj = timezone('Canada/Atlantic').localize(date_obj)
        else:
            raise Exception("Problem finding the timezone information->", self.filename)
        if self.debug:
            print('Date obj with timezone info:', date_obj)
        # convert all datetime to utc before writing to netcdf file
        date_obj = date_obj.astimezone(timezone('UTC'))
        return date_obj, date_obj.strftime('%Y/%m/%d %H:%M:%S.%f %Z')

    def fmt_len(self, fmt):
        # deprecated: calculated length of string from 'struct' format specification
        # assumes on 's' data fromat
        import numpy as np
        return np.asarray(fmt[0:-1].split('s'), dtype='int').sum()

    def get_data(self, formatline=None):
        # reads data using the information in FORMAT
        # if FORMAT information in file header is missing or does not work
        # then create 'struct' data format based on channel details information
        import fortranformat as ff
        import numpy as np
        import struct
        idx = self.find_index('*END OF HEADER')
        lines = self.lines[idx+1:]
        data = []
        if formatline is None:
            if self.debug:
                print("Reading data using format", self.channel_details['fmt_struct'])
            fmt_len = self.fmt_len(self.channel_details['fmt_struct'])
            fmt_struct = self.channel_details['fmt_struct']
            for i in range(len(lines)):
                if len(lines[i]) > 0:
                    # py2-3 migration...
                    # data.append(struct.unpack(self.channel_details['fmt_struct'], lines[i].rstrip().ljust(fmt_len)))
                    data.append(struct.unpack(fmt_struct, lines[i].rstrip().ljust(fmt_len).encode('utf-8')))
                    # data.append([r for r in lines[i].split()])
        else:
            ffline = ff.FortranRecordReader(formatline)
            for i in range(len(lines)):
                if len(lines[i]) > 0:
                    data.append([float(r) for r in ffline.read(lines[i])])
        data = np.asarray(data)
        return data

    def get_location(self):
        # read 'LOCATION' section from ios header
        # convert lat and lon to standard format (float, -180 to +180)
        # initialize some other standard section variables if possible
        info = self.get_section('LOCATION')
        if self.debug:
            print("Location details", info.keys())
    # handle lat conversion
        c = info['LATITUDE'].split()
        buf = float(c[0]) + float(c[1]) / 60.0
        if c[2] == 'S':
            info['LATITUDE'] = -1.0 * buf
        else:
            info['LATITUDE'] = buf
        c = info['LONGITUDE'].split()
    # handle lon conversion
        buf = float(c[0]) + float(c[1]) / 60.0
        if c[2] == 'W':
            info['LONGITUDE'] = -1.0 * buf
        else:
            info['LONGITUDE'] = buf
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
        lines = self.get_subsection('TABLE: CHANNEL DETAIL', self.FILE)
        mask = lines[1].rstrip()
        info = {}
        ch_det = [self.apply_col_mask(l, mask) for l in lines[2:]]
        info['Pad'] = [l[1] for l in ch_det]
        info['Width'] = [l[3] for l in ch_det]
        info['Format'] = [l[4] for l in ch_det]
        info['Type'] = [l[5] for l in ch_det]
        if int(self.FILE['NUMBER OF CHANNELS']) != len(info['Pad']):
            raise Exception('Number of channels in file record does not match channel_details!')
        else:
            fmt = ''
            for i in range(len(info['Pad'])):
                if info['Type'][i].strip() == 'D':
                    fmt = fmt+'11s'
                elif info['Type'][i].strip() == 'T':
                    fmt = fmt+'9s'
                else:
                    fmt = fmt + info['Width'][i].strip()+'s'

            info['fmt_struct'] = fmt
        if self.debug:
            print ("Python compatible data format:", fmt)
        return info

    def get_channels(self):
        # get the details of al the channels in the file
        # return as dictionary with each column as list
        lines = self.get_subsection('TABLE: CHANNELS', self.FILE)
        mask = lines[1].rstrip()
        info = {}
        ch = [self.apply_col_mask(l, mask) for l in lines[2:]]
        info['Name'] = [l[1] for l in ch]
        info['Units'] = [l[2] for l in ch]
        info['Minimum'] = [l[3] for l in ch]
        info['Maximum'] = [l[4] for l in ch]
        return info

    def apply_col_mask(self, data, mask):
        # apply mask to string (data) to get columns
        # return list of columns
        if self.debug:
            print(data, mask)
        data = data.rstrip().ljust(len(mask))
        a = [d == '-' for d in mask]
        ret = []
        for i in range(len(data)):
            if not a[i]:
                ret.append('*')
            else:
                ret.append(data[i])
        buf = ''.join(ret).split('*')
        while("" in buf):
            buf.remove("")
        return buf

    def get_comments_like(self, section_name):
        # to read sections like comments/remarks etc that are at 'root' level 
        # and contain a lot of information that must be kept together
        # return information as a dictionary with identifier being line number
        if section_name[0] != '*':
            section_name = '*'+section_name.strip()
        idx = self.find_index(section_name)
        if idx == -1:
            return ''
        info = {}
        # EOS = False # end of section logical
        count = 0
        while True:
            idx += 1
            count += 1
            l = self.lines[idx]
            if len(l.strip()) == 0: # skip line if blank
                continue
            elif l[0] == '!':
                continue
            elif l[0] in ['$', '*']:
                break
            else:
                if self.debug:
                    print(l)
                info['{:d}'.format(count)] = l.rstrip()
        return info

    def get_list_of_sections(self):
        # parse the entire header and returns list of sections available
        # skip first 2 lines of file (that has date and ios_header_version)
        # skip * in beginning of section name
        sections_list = []
        for i, line in enumerate(self.lines[2:]):
            if line[0] == '*' and line[0:4] != '*END' and line[1] != '*':
                sections_list.append(line.strip()[1:])
            else:
                continue
        return sections_list


class CtdFile(ObsFile):
    """
    Read CTD file in IOS format
    inherits methods from ObsFile class creates a new method called import_data
    this method processes files in manner that is specific to CTD dataset
    Author: Pramod Thupaki pramod.thupaki@hakai.org
    """
    def import_data(self):
        self.start_dateobj, self.start_date = self.get_date(opt='start')
        self.LOCATION = self.get_location()
        self.CHANNELS = self.get_channels()
        self.COMMENTS = self.get_comments_like('COMMENTS')
        self.REMARKS = self.get_comments_like('REMARKS')
        self.ADMINISTRATION = self.get_section('ADMINISTRATION')
        self.INSTRUMENT = self.get_section('INSTRUMENT')
        try:
            self.data = self.get_data(formatline=self.FILE['FORMAT'])
        except Exception as e:
            self.channel_details = self.get_channel_detail()
            self.data = self.get_data(formatline=None)
        if self.data is None:
            raise Exception("Error: Could not read data from format specified and could not decipher format")
            sys.exit()
        return 1


class CurFile(ObsFile):
    """
    Read current meter file in IOS format
    """
    pass

class MCtdFile(ObsFile):
    """
    Read Mooring CTD file in IOS format
    inherits methods from ObsFile class creates a new method called import_data
    this method processes files in manner that is specific to CTD dataset
    Author: Pramod Thupaki pramod.thupaki@hakai.org
    """
    def import_data(self):
        from datetime import timedelta
        startdateobj, self.start_date = self.get_date(opt='start')
        self.LOCATION = self.get_location()
        self.CHANNELS = self.get_channels()
        self.COMMENTS = self.get_comments_like('COMMENTS')
        self.REMARKS = self.get_comments_like('REMARKS')
        self.ADMINISTRATION = self.get_section('ADMINISTRATION')
        self.INSTRUMENT = self.get_section('INSTRUMENT')
        dt = self.get_dt()
        self.obs_time = [startdateobj+timedelta(seconds=dt*(i))
                        for i in range(int(self.FILE['NUMBER OF RECORDS']))]
        try:
            self.data = self.get_data(formatline=self.FILE['FORMAT'])
        except Exception as e:
            self.channel_details = self.get_channel_detail()
            self.data = self.get_data(formatline=None)
        if self.data is None:
            raise Exception("Error: Could not read data from format specified and could not decipher format")
            sys.exit()
        return 1


class BotFile(ObsFile):
    """
    Read bottle files in IOS format
    """
    pass
