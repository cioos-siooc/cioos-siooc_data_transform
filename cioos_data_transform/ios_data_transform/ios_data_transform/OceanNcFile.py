# class to hold data that will be written into ncfile
# includes methods to create ncfile in a standard format
# includes basic checks before ncfile is written
#
# AIM:  this will be the common entry point for data from different sources that go into CIOOS
#       ensuring common ncfile metadata standards. File has to conform to CF conventions and CIOOS variable standards
from netCDF4 import Dataset as ncdata


class OceanNcFile(object):
    def __init__(self):
        self.featureType = ''
        self.summary = ''
        self.title = ''
        self.institution = ''
        self.history = ''
        self.infoUrl = ''
        self.HEADER = ''
        # list of var class in the netcdf
        self.varlist = []
        self.nrec = 0

    def write_ncfile(self, ncfilename):
        # create ncfile
        self.ncfile = ncdata(filename=ncfilename, mode='w', format='NETCDF4', clobber=True)
        # setup global attributes of netcdf file based class data
        setattr(self.ncfile, 'featureType', self.featureType)
        setattr(self.ncfile, 'summary', self.summary)
        setattr(self.ncfile, 'title', self.title)
        setattr(self.ncfile, 'institution', self.institution)
        setattr(self.ncfile, 'history', self.history)
        setattr(self.ncfile, 'infoUrl', self.infoUrl)
        setattr(self.ncfile, 'HEADER', self.HEADER)
        # setup dimensions
        self.setup_dimensions()
        # setup attributes unique to the datatype
        self.setup_filetype()
        # write variables
        for var in self.varlist:
            self.__write_var(var)
        self.ncfile.close()

    def setup_dimensions(self):
        pass

    def setup_filetype(self):
        setattr(self.ncfile, 'cdm_profile_variables', '')

    def __write_var(self, var):
        # var.dimensions is a tuple
        # var.type is  a string
        # print('Writing', var.name, var.datatype, var.dimensions, var.data)
        ncvar = self.ncfile.createVariable(var.name, var.datatype, var.dimensions)
        for key, value in zip(['long_name', 'standard_name', 'units'],
                                [var.long_name, var.standard_name, var.units]):
            if value is not None:
                setattr(ncvar, key, value)
        # setattr(ncvar, 'long_name', var.long_name)
        # setattr(ncvar, 'standard_name', var.standard_name)
        # setattr(ncvar, 'units', var.units)
        if var.datatype == str:
            ncvar[0] = var.data
        else:
            setattr(ncvar, 'FillValue', float('NaN'))
            ncvar[:] = var.data


class CtdNcFile(OceanNcFile):
    def setup_dimensions(self):
        self.ncfile.createDimension('z', self.nrec)

    def setup_filetype(self):
        setattr(self.ncfile, 'cdm_profile_variables', 'time, profile')


class MCtdNcFile(OceanNcFile):
    def setup_dimensions(self):
        self.ncfile.createDimension('time', self.nrec)

    def setup_filetype(self):
        setattr(self.ncfile, 'cdm_timeseries_variables', 'profile')
