import re


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
