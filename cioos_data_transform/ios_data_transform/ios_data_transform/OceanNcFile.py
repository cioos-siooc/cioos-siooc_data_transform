# class to hold data that will be written into ncfile
# includes methods to create ncfile in a standard format
# includes basic checks before ncfile is written
#
# AIM:  this will be the common entry point for data from different sources that go into CIOOS
#       ensuring common ncfile metadata standards. File has to conform to CF conventions and CIOOS variable standards
from netCDF4 import Dataset as ncdata
import numpy as np


class OceanNcFile(object):
    def __init__(self):
        self.featureType = ""
        self.summary = ""
        self.summary_fra = ""
        self.title = ""
        self.title_fra = ""
        self.institution = ""
        self.history = ""
        self.infoUrl = ""
        self.header = ""
        self.description = ""
        self.description_fra = ""
        self.keywords = ""
        self.keywords_fra = ""
        self.acknowledgement = ""
        self.id = ""
        self.naming_authority = "COARDS,CF Standard Name Table v29"
        self.comment = ""
        self.creator_name = ""
        self.creator_email = ""
        self.creator_url = ""
        self.license = ""
        self.project = ""
        self.keywords_vocabulary = "GCMD Science Keywords"
        self.Conventions = "CF1.7,ACDD1.1"
        # list of var class in the netcdf
        self.varlist = []
        self.nrec = 0

    def write_ncfile(self, ncfilename):
        # create ncfile
        self.ncfile = ncdata(
            filename=ncfilename, mode="w", format="NETCDF4", clobber=True
        )
        # setup global attributes of netcdf file based class data
        setattr(self.ncfile, "featureType", self.featureType)
        for key in [
            "summary",
            "summary_fra",
            "title",
            "institution",
            "history",
            "infoUrl",
            "header",
            "description",
            "description_fra",
            "keywords",
            "keywords_fra",
            "acknowledgement",
            "id",
            "naming_authority",
            "comment",
            "creator_name",
            "creator_email",
            "creator_url",
            "license",
            "project",
            "keywords_vocabulary",
            "Conventions",
        ]:
            value = getattr(self, key)
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

    def setup_dimensions(self):
        pass

    def setup_filetype(self):
        setattr(self.ncfile, "cdm_profile_variables", "")

    def __write_var(self, var):
        # var.dimensions is a tuple
        # var.type is  a string
        # print('Writing', var.name, var.datatype, var.dimensions, var.data)
        fill_value = None
        if var.datatype is not str:
            fill_value = np.nan
        ncvar = self.ncfile.createVariable(
            var.name, var.datatype, var.dimensions, fill_value=fill_value
        )

        for key, value in zip(
            ["long_name", "standard_name", "units"],
            [var.long_name, var.standard_name, var.units],
        ):
            if value is not None:
                setattr(ncvar, key, value)

        if var.datatype == str:
            ncvar[0] = var.data
        else:
            ncvar[:] = var.data


class CtdNcFile(OceanNcFile):
    def setup_dimensions(self):
        self.ncfile.createDimension("z", self.nrec)

    def setup_filetype(self):
        setattr(self.ncfile, "cdm_profile_variables", "time, profile")


class MCtdNcFile(OceanNcFile):
    def setup_dimensions(self):
        self.ncfile.createDimension("time", self.nrec)

    def setup_filetype(self):
        setattr(self.ncfile, "cdm_timeseries_variables", "profile")


class CurNcFile(OceanNcFile):
    def setup_dimensions(self):
        self.ncfile.createDimension("time", self.nrec)

    def setup_filetype(self):
        setattr(self.ncfile, "cdm_timeseries_variables", "profile")
