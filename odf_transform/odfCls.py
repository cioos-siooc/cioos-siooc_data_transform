from ios_data_transform.OceanNcVar import OceanNcVar
from ios_data_transform.OceanNcFile import OceanNcFile
import numpy as np
from netCDF4 import Dataset as ncdata


class CtdNcFile(OceanNcFile):
    def setup_dimensions(self):
        self.ncfile.createDimension("z", self.nrec)

    def setup_filetype(self):
        setattr(self.ncfile, "cdm_profile_variables", "time, profile")

    def write_ncfile(self, ncfilename):
        # create ncfile
        self.ncfile = ncdata(filename=ncfilename, mode="w", format="NETCDF4", clobber=True)
        # setup global attributes of netcdf file based class data
        setattr(self.ncfile, "featureType", self.featureType)
        for featureName, featureVal in zip(
            [
                "summary",
                "title",
                "institution",
                "history",
                "infoUrl",
                "header",
                "description",
            ],
            [
                self.summary,
                self.title,
                self.institution,
                self.history,
                self.infoUrl,
                self.header,
                self.description,
            ],
        ):
            if featureVal is not None:
                setattr(self.ncfile, featureName, featureVal)
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
        fill_value = None
        if var.datatype is not str:
            fill_value = np.nan
        ncvar = self.ncfile.createVariable(
            var.name, var.datatype, var.dimensions, fill_value=fill_value
        )
        for key, value in zip(
            ["long_name", "standard_name", "units", "pcode", "gf3"],
            [var.long_name, var.standard_name, var.units, var.pcode, var.gf3],
        ):
            if value is not None:
                setattr(ncvar, key, value)
        if var.datatype == str:
            ncvar[0] = var.data
        else:
            ncvar[:] = var.data


class NcVar(OceanNcVar):
    def __init__(
        self,
        vartype,
        varname,
        varunits,
        varval,
        varclslist=[],
        vardim=(),
        varnull=float("nan"),
        conv_to_BODC=True,
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
        # from existing varlist. get all variables that are going to be written into the ncfile
        # this will be checked to make sure new variable name does not conflict with existing ones
        varlist = []
        for v in varclslist:
            varlist.append(v.name)
        self.add_var(varlist)

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
