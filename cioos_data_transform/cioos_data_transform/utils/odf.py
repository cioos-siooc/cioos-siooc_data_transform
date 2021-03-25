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
    pcode_att = odf_var_header_prefix + odf_variable_name

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
            odf_pcode = metadata[var][pcode_att]
            pcode = odf_pcode.rsplit('_', 1)  # Separate the pcode from the number at the end of the variable

            # Retrieve trailing number
            # pcodes are generally associated with a trailing number the define the primary, secondary ,... data
            metadata[var]['pcode'] = pcode[0]
            if len(pcode) == 2:
                metadata[var]['pcode_number'] = int(pcode[1])
            else:
                metadata[var]['pcode_number'] = 1

            # FLAG VARIABLES Detect if it is a flag column associated with another column
            flag_column = False
            if pcode[0].startswith('QQQQ'):  # MLI FLAG should apply to previous variable
                flag_dict[odf_pcode] = _find_previous_key(metadata, var)
                flag_column = True
            elif pcode[0].startswith('Q') and odf_pcode[1:] in metadata.keys():
                # BIO Format which Q+[PCODE] of the associated variable
                flag_dict[odf_pcode] = odf_pcode[1:]
                flag_column = True

            # Loop through each organisations and find the matching pcode within the vocabulary
            found_matching_vocab = False
            for organization in organizations:
                if pcode[0] in vocabulary[organization]:
                    vocab_attributes = {key: value for key, value in vocabulary[organization][pcode[0]].items()
                                        if key in vocabulary_attribute_list}
                    metadata[var].update(vocab_attributes)
                    found_matching_vocab = True
                    break  # Stop after the first one detected

            # If will get there if no matching vocabulary exist
            if not found_matching_vocab and not flag_column:
                print(str(pcode) + ' not available for organization: ' + str(organizations))

            # TODO compare expected units to units saved within the ODF file to make sure it is matching the vocabulary

    # Add Flag specific attributes
    for flag_column, data_column in flag_dict.items():
        if data_column in metadata:
            if 'name' in metadata[data_column]:
                metadata[flag_column]['long_name'] = flag_prefix + metadata[data_column]['name']
            else:
                metadata[flag_column]['long_name'] = flag_prefix + data_column
        # TODO improve flag parameters default documentation
        #  - add ancillary_variables to the associated variable attributes
        #       http://cfconventions.org/cf-conventions/cf-conventions.html#ancillary-data
        #  - add flag_values, flag_masks and flag_meanings to flag attributes
        #       http://cfconventions.org/cf-conventions/cf-conventions.html#flags

    # null_values / fill_values
    # Deal with fill value
    for key, var in metadata.items():
        if 'original_NULL_VALUE' in var.keys():

            if var['original_TYPE'] not in ['SYTM', 'INTE']:
                null_value = np.array(var['original_NULL_VALUE']) \
                    .astype(odf_dtypes[var['original_TYPE']])
            elif var['original_TYPE'] == 'SYTM' and \
                    re.match(r'\d\d-\w\w\w-\d\d\d\d\s\d\d\:\d\d\:\d\d', var['original_NULL_VALUE']):
                null_value = (dt.datetime.strptime(var['original_NULL_VALUE'],
                                              '%d-%b-%Y %H:%M:%S.%f') - dt.datetime(1970, 1, 1)).total_seconds()
            elif var['original_TYPE'] == 'INTE':
                null_value = int(np.array(var['original_NULL_VALUE']).astype(float).round())

            metadata[key]['null_value'] = null_value

    # Update P01 name based on pcode number
    for var in metadata:
        if 'sdn_parameter_urn' in metadata[var] and \
                type(metadata[var]['sdn_parameter_urn']) is str:
            metadata[var]['sdn_parameter_urn'] = re.sub(r'\d\d$',
                                                        '%02d' % metadata[var]['pcode_number'],
                                                        metadata[var]['sdn_parameter_urn'])
        if 'name' in metadata[var] and \
                type(metadata[var]['name']) is str:
            metadata[var]['name'] = re.sub(r'\d\d$',
                                           '%02d' % metadata[var]['pcode_number'],
                                           metadata[var]['name'])

    # TODO Add Warning for missing information and attributes (maybe)
    #  Example: Standard Name, P01, P02
    return metadata
