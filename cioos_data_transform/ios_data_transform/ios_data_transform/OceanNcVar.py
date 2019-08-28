# class to describe any variable that goes into a netcdf file
# will include bodc code generation
from datetime import datetime
from pytz import timezone
import numpy as np


class OceanNcVar(object):
    def __init__(self, vartype, varname, varunits, varmin, varmax, varval, varclslist=[], vardim=()):
        self.cf_role = None
        self.name = varname
        self.type = vartype
        self.standard_name = None
        self.long_name = None
        self.units = varunits
        self.maximum = varmin
        self.minimum = varmax
        self.datatype = ''
        self.dimensions = vardim
        self.data = varval
        # from existing varlist. get all variables that are going to be written into the ncfile
        # this will be checked to make sure new variable name does not conflict with existing ones
        varlist = []
        for v in varclslist:
            varlist.append(v.name)
        self.add_var(varlist)

    def add_var(self, varlist):
        """
        add variable to netcdf file using variables passed as inputs
        author: Pramod Thupaki pramod.thupaki@hakai.org
        input:
            ncfile: Dataset object where variables will be added
            vartype: variable type
            varname: nominal name of variable being passed. this can be IOS_dataname.
                    can be used to determine BODC codes
            varunits: Units specifications from IOS file
            varmin: minimum value of variable as specified in IOS file
            varmax: maximum value of variable as specified in IOS file
        output:
            NONE
        """
        if self.type == 'str_id':
            self.datatype = str
        elif self.type == 'profile':
            self.datatype = str
            self.cf_role = 'profile_id'
        elif self.type == 'instr_depth':
            self.datatype = 'float32'
            self.long_name = 'Instrument Depth'
            self.standard_name = 'instrument_depth'
            self.units = 'm'
        elif self.type == 'lat':
            self.datatype = 'float32'
            self.long_name = 'Latitude'
            self.standard_name = 'latitude'
            self.units = 'degrees_north'
        elif self.type == 'lon':
            self.datatype = 'float32'
            self.long_name = 'Longitude'
            self.standard_name = 'latitude'
            self.units = 'degrees_east'
        elif self.type == 'time':
            self.datatype = 'double'
            self.standard_name = 'time'
            self.long_name = 'time'
            self.units = 'seconds since 1970-01-01 00:00:00+0000'
            dt = np.asarray(self.data) # datetime.datetime.strptime(self.data, '%Y/%m/%d %H:%M:%S.%f %Z')
            buf = dt - timezone('UTC').localize(datetime(1970, 1, 1, 0, 0, 0))
            self.data = [i.total_seconds() for i in buf]
            # self.data = (dt - datetime.datetime(1970, 1, 1).astimezone(timezone('UTC'))).total_seconds()
        elif self.type == 'depth':
            self.datatype = 'float32'
            # self.dimensions = ('z')
            self.long_name = 'Depth in meters'
            self.standard_name = 'depth_below_sea_level_in_meters'
        elif self.type == 'pressure':
            self.name = 'PRESPR01'
            self.datatype = 'float32'
            # self.dimensions = ('z')
            self.long_name = 'Pressure'
            if self.units.strip().lower() in ['dbar', 'decibar']:
                self.units = 'decibar'
            else:
                raise Exception('Unclear units for pressure!')
            self.standard_name = 'sea_water_pressure'
        elif self.type == 'temperature':
            self.datatype = 'float32'
            # self.dimensions = ('z')
            for i in range(4):
                bodc_code, bodc_units = self.__get_bodc_code(self.type, self.name, self.units, i)
                if bodc_code not in varlist:
                    break
            self.name = bodc_code
            self.long_name = 'Sea Water Temperature'
            self.standard_name = 'sea_water_temperature'
            self.units = bodc_units
        elif self.type == 'salinity':
            self.datatype = 'float32'
            # self.dimensions = ('z')
            for i in range(4): # will try to get a unique variable name at least 4 times
                bodc_code, bodc_units = self.__get_bodc_code(self.type, self.name, self.units, i)
                if bodc_code not in varlist:
                    # var = ncfile.createVariable(bodc_code, 'float32', ('z'))
                    break
            self.name = bodc_code
            self.long_name = 'Sea Water Practical Salinity'
            self.standard_name = 'sea_water_practical_salinity'
            self.units = bodc_units
        elif self.type == 'oxygen':
            self.datatype = 'float32'
            # self.dimensions = ('z')
            for i in range(4): # will try to get a unique variable name at least 4 times
                bodc_code, bodc_units = self.__get_bodc_code(self.type, self.name, self.units, i)
                if bodc_code not in varlist:
                    break
            self.name = bodc_code
            self.long_name = 'Oxygen concentration'
            self.standard_name = 'dissolved_oxygen_concentration'
            self.units = bodc_units
        elif self.type == 'conductivity':
            self.datatype = 'float32'
            # self.dimensions = ('z')
            for i in range(4): # will try to get a unique variable name at least 4 times
                bodc_code, bodc_units = self.__get_bodc_code(self.type, self.name, self.units, i)
                if bodc_code not in varlist:
                    break
            self.name = bodc_code
            self.long_name = 'Sea Water Electrical Conductivity'
            self.standard_name = 'sea_water_electrical_conductivity'
            self.units = bodc_units
        else:
            print("Do not know how to define this variable..")
            raise Exception("Fatal Error")

    def __get_bodc_code(self, vartype, ios_varname, varunits, iter):
        """
        return the correct BODC code based on variable type, units and ios variable name
        author: Pramod Thupaki pramod.thupaki@hakai.org
        inputs:
            varname:
            vartype: list. [0] = vartype, [1]=instance details (primary/secondary etc)
            varunits:
        output:
            BODC code
        """
        from .utils import is_in
        bodc_code = ''; bodc_units = ''
        if vartype == 'temperature':
            if is_in(['ITS90', 'ITS-90'], varunits):
                bodc_code = 'TEMPS9'; bodc_units = 'deg C'
            elif is_in(['IPTS-68', 'IPTS68'], varunits):
                bodc_code = 'TEMPS6'; bodc_units = 'deg C'
            elif is_in(['deg c', 'degc'], varunits):
                bodc_code = 'TEMPST'; bodc_units = 'deg C'
            else: # if varunits does not specify type of temperature
                raise Exception("Temperature type not defined", ios_varname, varunits, vartype)
            bodc_code = '{}{:02d}'.format(bodc_code, iter+1)
        elif vartype == 'salinity':
            if is_in(['PSS-78'], varunits):
                bodc_code = "PSALST"; bodc_units = 'PSS-78'
            elif is_in(['ppt'], varunits):
                bodc_code = "SSALST"; bodc_units = 'PPT'
            else:
                raise Exception("Salinity type not defined", ios_varname, varunits, vartype)
            bodc_code = '{}{:02d}'.format(bodc_code, iter+1)
        elif vartype == 'oxygen':
            if is_in(['ml/l'], varunits):
                bodc_code = "DOXYZZ"; bodc_units = 'mL/L'
            elif is_in(['umol/kg'], varunits):
                bodc_code = "DOXMZZ"; bodc_units = 'umol/kg'
            else:
                raise Exception("Oxygen units not found")
            bodc_code = '{}{:02d}'.format(bodc_code, iter+1)
        elif vartype == 'conductivity':
            if is_in(['s/m'], varunits):
                bodc_code = 'CNDCST'; bodc_units = 'S/m'
            else:
                raise Exception("Conductivity units not compatible with BODC code", ios_varname, varunits, vartype)
            bodc_code = '{}{:02d}'.format(bodc_code, iter+1)
        else:
            raise Exception('Cannot find BODC code for this variable', ios_varname, varunits, vartype)
        return bodc_code, bodc_units
