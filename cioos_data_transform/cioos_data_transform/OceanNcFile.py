# class to hold data that will be written into ncfile
# includes methods to create ncfile in a standard format
# includes basic checks before ncfile is written
#
# AIM:  this will be the common entry point for data from different sources that go into CIOOS
#       ensuring common ncfile metadata standards. File has to conform to CF conventions and CIOOS variable standards
from netCDF4 import Dataset as ncdata
import numpy as np
from .OceanNcVar import OceanNcVar


class OceanNcFile(object):
    def __init__(self):
        # list of var class in the netcdf
        self.varlist = []
        self.nrec = 0

    def write_ncfile(self, ncfilename):
        # create ncfile
        self.ncfile = ncdata(
            filename=ncfilename, mode="w", format="NETCDF4", clobber=True
        )
        # setup global attributes of netcdf file
        for key, value in self.global_attrs.items():
            if value is not None:
                setattr(self.ncfile, key, value)
        # setup dimensions
        self.setup_dimensions()
        # setup attributes unique to the datatype
        self.setup_filetype()
        # write variables
        # print("writing variables", len(self.varlist))
        for var in self.varlist:
            self.__write_var(var)
        self.ncfile.close()

    def setup_dimensions(self):
        pass

    def setup_filetype(self):
        setattr(self.ncfile, "cdm_profile_variables", "")

    def __write_var(self, var):
        # var.dimensions is a tuple
        # var.type is  a string
        # print("Writing", var.name, var.datatype, var.dimensions, var.data)
        fill_value = None
        if var.datatype is not str:
            fill_value = np.nan
        ncvar = self.ncfile.createVariable(
            var.name, var.datatype, var.dimensions, fill_value=fill_value
        )

        for key in ["long_name", "standard_name", "units"]:
            value = getattr(var, key)
            if value:
                setattr(ncvar, key, value)
        # additional (optional) attributes
        for key, value in var.attributes.items():
            if value:
                setattr(ncvar, key, value)

        if var.datatype == str:
            ncvar[0] = var.data
        else:
            ncvar[:] = var.data

    def add_var(
        self,
        vartype,
        varname,
        varunits,
        varval,
        vardim=(),
        varnull=float("nan"),
        conv_to_BODC=True,
        attributes={},
    ):

        varnames = list(map(lambda var: var.name, self.varlist))

        nc_var = OceanNcVar(
            vartype,
            varname,
            varunits,
            varval,
            vardim,
            varnull,
            conv_to_BODC,
            attributes,
        )

        nc_var.add_var(varnames)

        self.varlist.append(nc_var)


class CtdNcFile(OceanNcFile):
    def setup_dimensions(self):
        self.ncfile.createDimension("z", self.nrec)

    # def setup_filetype(self):
    #     setattr(self.ncfile, "cdm_profile_variables", "time, profile")


class MCtdNcFile(OceanNcFile):
    def setup_dimensions(self):
        self.ncfile.createDimension("time", self.nrec)

    # def setup_filetype(self):
    #     setattr(self.ncfile, "cdm_timeseries_variables", "profile")


class CurNcFile(OceanNcFile):
    def setup_dimensions(self):
        self.ncfile.createDimension("time", self.nrec)

    # def setup_filetype(self):
    #     setattr(self.ncfile, "cdm_timeseries_variables", "profile")
