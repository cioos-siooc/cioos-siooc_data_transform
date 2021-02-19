import re
import warnings


def convert_oce_units_to_udunit(units):
    """
    OCE convert ODF units to a special list. This present tool tries to convert the OCE units format
     to a udunit format.
    """
    class ud:
        def __init__(self, unit='', exponent=1):
            self.unit = unit
            self.exponent = exponent

        def reverse_exponent(self):
            self.exponent = -1 * self.exponent
            return self

        def read_unit(self, input):
            if isinstance(input, str):
                self.unit = str(input)
            elif isinstance(input, list):
                if input[0] == '^':
                    self.read_exponent_var(input)
                else:
                    self.unit = ''.join(input)
            return self

        def read_exponent_var(self, input):
            if input[0] == '^':
                self.unit = input[1]
                self.exponent = input[2]
            else:
                raise RuntimeError('Failed to read expoenent:' + str(input))
            return self

        def write_udunit(self):
            exp_str = ''
            if self.exponent != 1:
                exp_str = str(self.exponent)
            return self.unit+exp_str

    # Loop through each units
    for var, unit in units.items():
        unit_list = []
        # Try to convert OCE units to udunits go through each cell of unit
        try:
            for item in unit['unit']:
                # If it's not a fraction list not ['/',a,b]
                if isinstance(item, str) or isinstance(item, list) and item[0] not in ['/'] :
                    unit_list.append(ud().read_unit(item))
                # If it is a fraction
                elif isinstance(item, list) and len(item) == 3 and item[0] in ['/']:
                    # If the numerator has also a fraction
                    if isinstance(item[1], list) and len(item[1]) == 3 and item[1][0] in ['/']:
                        unit_list.append(ud().read_unit(item[1][1]))
                        unit_list.append(ud().read_unit(item[1][2]).reverse_exponent())
                    else:
                        unit_list.append(ud().read_unit(item[1]))
                    # If the denominator has also a fraction
                    if isinstance(item[2], list) and len(item[2]) == 3 and item[2][0] in ['/']:
                        unit_list.append(ud().read_unit(item[2][1]).reverse_exponent())
                        unit_list.append(ud().read_unit(item[2][2]))
                    else:
                        unit_list.append(ud().read_unit(item[2]).reverse_exponent())

            units[var]['udunit'] = ' '.join([item.write_udunit() for item in unit_list])
            units[var]['udunit'] = units[var]['udunit'].replace('einstein', 'E')
        except TypeError:
            raise Warning('Fail to convert OCE unit '+str(unit['unit'])+' to udunit')
    return units


def get_odf_var_attributes_to_oce(metadata,
                                  oce_variable_parameter='CODE'):
    """
    OCE change completely the order of the original data, but is still providing access to the header
    metadata within a metadata dictionary. This tool retrieve the original header and assign variable name to each
     respective original header variable.
    """
    # OCE also rename the variable to some other names, we'll match the ODF header to OCE variables
    map_oce_variables = dict(zip(metadata['dataNamesOriginal'].values(),
                                 metadata['dataNamesOriginal'].keys()))
    odf_variable_header = {}
    for key, attribute_list in metadata["header"].items():
        if key.startswith('PARAMETER_HEADER'):
            # For some reasons OCE appen a number at the end of each attriutes (ex: _01), we'll get rid of them.
            attribute_list = {re.sub(r'_\d+$', '', att): parm for att, parm in attribute_list.items()}
            # Let's remove the \' symbols around the strings
            attribute_list = {att: re.sub(r'^\'|\'$', '', parm) for att, parm in attribute_list.items()}

            # # Convert float and integer in attributes
            # for att, parm in attribute_list.items():
            #     if re.match(r'[\+\-]{0,1}\d+\.\d+', parm):  # Is float
            #         attribute_list[att] = float(parm)
            #     elif re.match(r'\d+', parm):  # Is integer
            #         attribute_list[att] = int(parm)
            # TODO make this compatible with all ODF formats

            if oce_variable_parameter in attribute_list:
                var_name = attribute_list[oce_variable_parameter]
                # Retrieve OCE name otherwise keep the original name
                if var_name in map_oce_variables.keys():
                    odf_variable_header.update({map_oce_variables[var_name]: attribute_list})
                else:
                    odf_variable_header.update({var_name: attribute_list})

    return odf_variable_header
