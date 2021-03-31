import re
import datetime as dt
import numpy as np

def define_odf_variable_attributes(metadata,
                                   oce_metadata=None,
                                   organizations=None,
                                   vocabulary=None,
                                   vocabulary_attribute_list=None,
                                   odf_var_header_prefix='original_',
                                   odf_variable_name="CODE",
                                   flag_prefix='QualityFlag:'):
    """
    This method is use to retrieve from an ODF file each variable code and corresponding related
    vocabularies associated to the organization and variable name.
    Flag columns are also reviewed and matched to the appropriate variable.
    """
    # Make sure organization is a list
    if type(organizations) is str:
        organizations = [organizations]
    # Define vocabulary default list of variables to import as attributes
    if vocabulary_attribute_list is None:
        vocabulary_attribute_list = ['name', 'standard_name',
                                     'sdn_parameter_urn', 'sdn_parameter_name',
                                     'sdn_uom_urn', 'sdn_uom_name']
    parameter_code_att = odf_var_header_prefix + odf_variable_name

    def _find_previous_key(key_list, key):
        """
        For some ODF format, a flag column is related to the variable prior to it. This tool is use to retrieve
        the name of this variable situated prior to a given column.
        """
        previous_key = ''
        for i in key_list.keys():
            if i == key:
                break
            else:
                previous_key = i
        return previous_key

    # Find matching vocabulary
    if type(vocabulary) is dict:
        flag_dict = {}
        for var in metadata.keys():
            # Retrieve ODF CODE and Associated number
            odf_paraneter_code = metadata[var][parameter_code_att]
            # Separate the parameter_code from the number at the end of the variable
            parameter_code = odf_paraneter_code.rsplit('_', 1)

            # Retrieve trailing number
            # Parameter codes are generally associated with a trailing number the define the
            # primary, secondary ,... data
            metadata[var]['parameter_code'] = parameter_code[0]
            if len(parameter_code) == 2:
                metadata[var]['parameter_code_number'] = int(parameter_code[1])
            else:
                metadata[var]['parameter_code_number'] = 1

            # FLAG VARIABLES Detect if it is a flag column associated with another column
            flag_column = False
            if parameter_code[0].startswith('QQQQ'):  # MLI FLAG should apply to previous variable
                flag_dict[odf_paraneter_code] = _find_previous_key(metadata, var)
                flag_column = True
            elif parameter_code[0].startswith('Q') and odf_paraneter_code[1:] in metadata.keys():
                # BIO Format which Q+[PCODE] of the associated variable
                flag_dict[odf_paraneter_code] = odf_paraneter_code[1:]
                flag_column = True
            # Make sure that the flag column relate to something
            if flag_column and flag_dict[odf_paraneter_code] not in metadata:
                raise UserWarning(odf_paraneter_code + ' flag is refering to' + \
                                  flag_dict[odf_paraneter_code] + ' which is not available as variable')

            # Loop through each organisations and find the matching parameter_code within the vocabulary
            found_matching_vocab = False
            for organization in organizations:
                if parameter_code[0] in vocabulary[organization]:
                    vocab_attributes = {key: value for key, value in vocabulary[organization][parameter_code[0]].items()
                                        if key in vocabulary_attribute_list}
                    metadata[var].update(vocab_attributes)
                    found_matching_vocab = True
                    break  # Stop after the first one detected

            # If will get there if no matching vocabulary exist
            if not found_matching_vocab and not flag_column:
                print(str(parameter_code) + ' not available for organization: ' + str(organizations))

            # TODO compare expected units to units saved within the ODF file to make sure it is matching the vocabulary

    # Add Flag specific attributes
    for flag_column, data_column in flag_dict.items():
        if data_column in metadata:
            # Add long name attribute which is generally QUALITY_FLAG: [variable it affects]
            if 'name' in metadata[data_column]:
                metadata[flag_column]['long_name'] = flag_prefix + metadata[data_column]['name']
            else:
                metadata[flag_column]['long_name'] = flag_prefix + data_column

            # Add ancillary_variables attribute
            if 'ancillary_variables' not in metadata[data_column]:
                metadata[data_column]['ancillary_variables'] = flag_column
            elif 'ancillary_variables' in metadata[data_column] and type(metadata[data_column]) is str:
                metadata[data_column]['ancillary_variables'] += ','+flag_column
            else:
                raise UserWarning('unknown ancillary flag format attribute')
        # TODO improve flag parameters default documentation
        #  - add flag_values, flag_masks and flag_meanings to flag attributes
        #       http://cfconventions.org/cf-conventions/cf-conventions.html#flags
        #       we would need to know the convention used by the organization if there's any.
        #       Otherwise, this should be implemented within the erddap dataset.

    # Update P01 name based on parameter_code number
    for var in metadata:
        if 'sdn_parameter_urn' in metadata[var] and \
                type(metadata[var]['sdn_parameter_urn']) is str:
            metadata[var]['sdn_parameter_urn'] = re.sub(r'\d\d$',
                                                        '%02d' % metadata[var]['parameter_code_number'],
                                                        metadata[var]['sdn_parameter_urn'])
        if 'name' in metadata[var] and \
                type(metadata[var]['name']) is str:
            metadata[var]['name'] = re.sub(r'\d\d$',
                                           '%02d' % metadata[var]['parameter_code_number'],
                                           metadata[var]['name'])

    # TODO Add Warning for missing information and attributes (maybe)
    #  Example: Standard Name, P01, P02
    return metadata
