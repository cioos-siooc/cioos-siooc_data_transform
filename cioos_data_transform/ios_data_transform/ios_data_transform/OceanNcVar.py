# class to describe any variable that goes into a netcdf file
# will include bodc code generation
from datetime import datetime
from pytz import timezone
import numpy as np


class OceanNcVar(object):
    def __init__(self, vartype, varname, varunits, varmin, varmax, varval, varclslist=[], vardim=(),
                 varnull=float("nan"), conv_to_BODC = True):
        self.cf_role = None
        self.name = varname
        self.type = vartype
        self.standard_name = None
        self.long_name = None
        self.units = varunits
        self.maximum = varmin
        self.minimum = varmax
        self.datatype = ''
        self.null_value = varnull
        self.dimensions = vardim
        self.data = varval
        self.conv_to_BODC = conv_to_BODC
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
            dt = np.asarray(self.data)  # datetime.datetime.strptime(self.data, '%Y/%m/%d %H:%M:%S.%f %Z')
            buf = dt - timezone('UTC').localize(datetime(1970, 1, 1, 0, 0, 0))
            self.data = [i.total_seconds() for i in buf]
            # self.data = (dt - datetime.datetime(1970, 1, 1).astimezone(timezone('UTC'))).total_seconds()
        elif self.type == 'depth':
            self.datatype = 'float32'
            # self.dimensions = ('z')
            self.long_name = 'Depth in meters'
            self.standard_name = 'depth_below_sea_level_in_meters'
            self.units = 'm'
            self.__set_null_val()
        elif self.type == 'pressure':
            self.name = 'PRESPR01'
            self.datatype = 'float32'
            # self.dimensions = ('z')
            self.long_name = 'Pressure'
            if self.units.strip().lower() in ['dbar', 'dbars', 'decibar']:
                self.units = 'decibar'
            else:
                raise Exception('Unclear units for pressure!')
            self.standard_name = 'sea_water_pressure'
            self.__set_null_val()
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
            self.__set_null_val()
        elif self.type == 'salinity':
            self.datatype = 'float32'
            # self.dimensions = ('z')
            for i in range(4):  # will try to get a unique variable name at least 4 times
                bodc_code, bodc_units = self.__get_bodc_code(self.type, self.name, self.units, i)
                if bodc_code not in varlist:
                    break
            self.name = bodc_code
            self.long_name = 'Sea Water Practical Salinity'
            self.standard_name = 'sea_water_practical_salinity'
            self.units = bodc_units
            self.__set_null_val()
        elif self.type == 'oxygen':
            self.datatype = 'float32'
            # self.dimensions = ('z')
            for i in range(4):  # will try to get a unique variable name at least 4 times
                bodc_code, bodc_units = self.__get_bodc_code(self.type, self.name, self.units, i)
                if bodc_code not in varlist:
                    break
            self.name = bodc_code
            self.long_name = 'Oxygen concentration'
            self.standard_name = 'dissolved_oxygen_concentration'
            self.units = bodc_units
            self.__set_null_val()
        elif self.type == 'conductivity':
            self.datatype = 'float32'
            # self.dimensions = ('z')
            for i in range(4):  # will try to get a unique variable name at least 4 times
                bodc_code, bodc_units = self.__get_bodc_code(self.type, self.name, self.units, i)
                if bodc_code not in varlist:
                    break
            self.name = bodc_code
            self.long_name = 'Sea Water Electrical Conductivity'
            self.standard_name = 'sea_water_electrical_conductivity'
            self.units = bodc_units
            self.__set_null_val()
        elif self.type == 'nutrient':
            self.datatype = 'float32'
            for i in range(4):
                bodc_code, bodc_units = self.__get_bodc_code(self.type, self.name, self.units, i)
                if bodc_code not in varlist:
                    break
            self.name = bodc_code
            self.units = bodc_units
            self.__set_null_val()
        elif self.type == 'other':
            self.datatype = 'float32'
            for i in range(4):
                bodc_code, bodc_units = self.__get_bodc_code(self.type, self.name, self.units, i)
                if bodc_code not in varlist:
                    break
            self.name = bodc_code
            self.units = bodc_units
            self.__set_null_val()
        else:
            print("Do not know how to define this variable..")
            raise Exception("Fatal Error")

    def __set_null_val(self):
        self.data = np.asarray(self.data, dtype=float)
        try:
            self.data[self.data == float(self.null_value)] = float("nan")
        except Exception as e:
            print("Pad field is empty. Setting FillValue to NaN ...")

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
        if not self.conv_to_BODC:
            # do not convert varname, varunits ot BODC. instead just return original values
            # use this option on datasets where variables are not named using this convention or
            # using a different method to define variable names and types
            return self.name, self.units

        bodc_code = ''
        bodc_units = ''
        if vartype == 'temperature':
            if is_in(['reversing'], ios_varname) and is_in(['deg c'], varunits):
                bodc_code = 'TEMPRTN'
                bodc_units = 'deg C'
                bodc_code = '{}{:01d}'.format(bodc_code, iter + 1)
            elif is_in(['ITS90', 'ITS-90'], varunits):
                bodc_code = 'TEMPS9'
                bodc_units = 'deg C'
                bodc_code = '{}{:02d}'.format(bodc_code, iter + 1)
            elif is_in(['IPTS-68', 'IPTS68'], varunits):
                bodc_code = 'TEMPS6'
                bodc_units = 'deg C'
                bodc_code = '{}{:02d}'.format(bodc_code, iter + 1)
            elif is_in(['deg c', 'degc'], varunits):
                bodc_code = 'TEMPST'
                bodc_units = 'deg C'
                bodc_code = '{}{:02d}'.format(bodc_code, iter + 1)
            else:  # if varunits does not specify type of temperature
                raise Exception("Temperature type not defined", ios_varname, varunits, vartype)

        elif vartype == 'salinity':
            if not is_in(['bottle'], ios_varname) and is_in(['PSS-78'], varunits):
                bodc_code = "PSALST"
                bodc_units = 'PSS-78'
                bodc_code = '{}{:02d}'.format(bodc_code, iter + 1)
            elif not is_in(['bottle'], ios_varname) and is_in(['ppt'], varunits):
                bodc_code = "SSALST"
                bodc_units = 'PPT'
                bodc_code = '{}{:02d}'.format(bodc_code, iter + 1)
            elif is_in(['bottle'], ios_varname) and is_in(['PSS-78'], varunits):
                bodc_code = "PSALBST"
                bodc_units = 'PSS-78'
                bodc_code = '{}{:01d}'.format(bodc_code, iter + 1)
            elif is_in(['bottle'], ios_varname) and is_in(['ppt'], varunits):
                bodc_code = "ODSDM021"
                bodc_units = 'PPT'

            else:
                raise Exception("Salinity type not defined", ios_varname, varunits, vartype)

        elif vartype == 'oxygen':
            if is_in(['ml/l'], varunits):
                bodc_code = "DOXYZZ"
                bodc_units = 'mL/L'
            elif is_in(['umol/kg'], varunits):
                bodc_code = "DOXMZZ"
                bodc_units = 'umol/kg'
            elif is_in(['umol/L'], varunits):
                bodc_code = "DOXY"
                bodc_units = 'umol/L'
            else:
                raise Exception("Oxygen units not defined", ios_varname, varunits, vartype)
            bodc_code = '{}{:02d}'.format(bodc_code, iter + 1)
        elif vartype == 'conductivity':
            if is_in(['s/m'], varunits):
                bodc_code = 'CNDCST'
                bodc_units = 'S/m'
            elif is_in(['ms/cm'], varunits):
                bodc_code = 'CNDCSTX'
                bodc_units = 'mS/cm'
            else:
                raise Exception("Conductivity units not compatible with BODC code", ios_varname, varunits, vartype)
            bodc_code = '{}{:02d}'.format(bodc_code, iter + 1)
        elif vartype == 'nutrient':
            if is_in(['nitrate_plus_nitrite'], ios_varname) and is_in(['umol/l'], varunits):
                bodc_code = 'NTRZAAZ'
                bodc_units = 'umol/L'
                self.standard_name = 'mole_concentration_of_nitrate_and_nitrite_in_sea_water'
                self.long_name = 'Mole Concentration of Nitrate and Nitrite in Sea Water'
            elif is_in(['phosphate'], ios_varname) and is_in(['umol/l'], varunits):
                bodc_code = 'PHOSAAZ'
                bodc_units = 'umol/L'
                self.standard_name = 'mole_concentration_of_phosphate_in_sea_water'
                self.long_name = 'Mole Concentration of Phosphate in Sea Water'
            elif is_in(['silicate'], ios_varname) and is_in(['umol/l'], varunits):
                bodc_code = 'SLCAAAZ'
                bodc_units = 'umol/L'
                self.standard_name = 'mole_concentration_of_silicate_in_sea_water'
                self.long_name = 'Mole Concentration of Silicate in Sea Water'
            else:
                raise Exception("Nutrient units not compatible with BODC code", ios_varname, varunits, vartype)
            bodc_code = '{}{:01d}'.format(bodc_code, iter + 1)
        elif vartype == 'other':
            if is_in(['chlorophyll'], ios_varname) and is_in(['mg/m^3'], varunits):
                bodc_code = 'CPHLFLP'
                bodc_units = 'mg/m^3'
                self.standard_name = 'concentration_of_chlorophyll-a_in_water_body'
                self.long_name = 'Concentration of chlorophyll-a {chl-a CAS 479-61-8} per unit volume of the water body [particulate >GF/F phase] by filtration, acetone extraction and fluorometry'
            else:
                raise Exception("'Other' units not compatible with BODC code", ios_varname, varunits, vartype)
            bodc_code = '{}{:01d}'.format(bodc_code, iter + 1)
        else:
            raise Exception('Cannot find BODC code for this variable', ios_varname, varunits, vartype)
        return bodc_code, bodc_units
