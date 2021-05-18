import numpy as np
from netCDF4 import Dataset as ncdata
from .OceanNcFile import OceanNcFile
from .OceanNcVar import OceanNcVar


class CtdNcFile(OceanNcFile):
    def setup_dimensions(self):
        self.ncfile.createDimension("z", self.nrec)

    def setup_filetype(self):
        setattr(self.ncfile, "cdm_profile_variables", "time, profile")

    def write_ncfile(self, ncfilename):

        # create ncfile
        self.ncfile = ncdata(
            filename=ncfilename, mode="w", format="NETCDF4", clobber=True
        )
        # print(self.global_attrs)
        for key, value in self.global_attrs.items():
            if value is not None:
                setattr(self.ncfile, key, value)

        # setup dimensions
        self.setup_dimensions()
        # setup attributes unique to the datatype
        self.setup_filetype()
        # write variables
        for var in self.varlist:
            self.__write_var(var)
        self.ncfile.close()

    def __write_var(self, var):
        # var.dimensions is a tuple
        # var.type is  a string
        # print('Writing', var.name, var.datatype, var.dimensions)
        # fill_value = None
        # if var.datatype is not str:
        #     fill_value = np.nan
        ncvar = self.ncfile.createVariable(
            var.name, var.datatype, var.dimensions, fill_value=var.null_value
        )
        for key in ["long_name", "standard_name", "units", "pcode", "gf3"]:
            value = getattr(var, key)

            if value:
                setattr(ncvar, key, value)
        
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
        attributes={}
    ):

        varnames = list(map(lambda var: var.name, self.varlist))

        nc_var = NcVar(
            vartype, varname, varunits, varval, vardim, varnull, conv_to_BODC, attributes
        )

        nc_var.add_var(varnames)

        self.varlist.append(nc_var)


class NcVar(OceanNcVar):
    def __init__(
        self,
        vartype,
        varname,
        varunits,
        varval,
        vardim=(),
        varnull=float("nan"),
        conv_to_BODC=True,
        attributes={}
    ):
        self.cf_role = None
        self.name = varname
        self.type = vartype
        self.standard_name = None
        self.long_name = None
        self.units = varunits
        self.datatype = ""
        self.null_value = varnull
        self.dimensions = vardim
        self.data = varval
        self.conv_to_BODC = conv_to_BODC
        # add bodc, gf3, and pcode information
        self.gf3 = self.get_gf3()
        self.pcode = self.get_pcode()
        self.bodc = self.get_bodc()

        # NetCDF variable attributes
        self.attributes = attributes

    def get_bodc(self):
        # calculate the correct BODC, pcode, GF3 code
        # calculate standard name and long name
        # based on variable name, type, and units
        return ""

    def get_pcode(self):
        if self.name in [
            "filename",
            "country",
            "institute",
            "cruise_id",
            "scientist",
            "project",
            "platform",
            "instrument_type",
            "instrument_serial_number",
        ]:
            pcode = None
        else:
            pcode = ""
        return pcode

    def get_gf3(self):
        if self.name in [
            "filename",
            "country",
            "institute",
            "cruise_id",
            "scientist",
            "project",
            "platform",
            "instrument_type",
            "instrument_serial_number",
        ]:
            gf3 = None
        else:
            gf3 = ""
        return gf3
