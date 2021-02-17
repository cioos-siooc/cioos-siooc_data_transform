import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import pandas as pd
from tqdm import tqdm

# from tqdm.auto import tqdm  # for notebooks

# Create and register a new `tqdm` instance with `pandas`
# (can use tqdm_gui, optional kwargs, etc.)
tqdm.pandas()


def get_nvs_variable_info(id=None,
                          variable=None,
                          vocabulary=None,
                          nvs_url="http://vocab.nerc.ac.uk/collection/",
                          version="current",
                          format_output="?_profile=skos&_mediatype=application/ld+json"
                          ):
    """
    Method to parse the json format from the NERC NVS servers
    """
    if id:
        url = id
    else:
        # Define the base of the URL
        url = nvs_url + '/' + vocabulary + '/' + version

        # Add the optional variable
        if variable:
            url = url + '/' + variable

    # Get the response from the NERC servers
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    response = session.get(url + '/' + format_output)
    return response.json()


def split_nerc_id(id_url):
    """
    Small method to parse the NERC url for each variables to extract information
    """
    # Split the order ids to extract name and vocab
    id_list = id_url.split('/')
    val = ['http', 'empty', 'nerc_url', 'type', 'vocabulary', 'version', 'variable', 'unknown']
    return dict(zip(val, id_list))


def retrieve_variable_info(nerc_id):
    """
    Method used to extract important information from the variable specific info.
    """
    info = get_nvs_variable_info(nerc_id)

    if len(info)>1:
        assert RuntimeWarning('mutile info exists')

    var_dict = {}
    # Get Definition
    for definition in info[0]['http://www.w3.org/2004/02/skos/core#definition']:
        var_dict.update({'definition_' + definition['@language']: definition['@value']})
    # Pref Label
    for prefLabel in info[0]['http://www.w3.org/2004/02/skos/core#prefLabel']:
        var_dict.update({'prefLabel_' + prefLabel['@language']: prefLabel['@value']})
    # Broader P07 matching (CF Name)
    for id in info[0]['http://www.w3.org/2004/02/skos/core#broader']:
        if '/P07/' in id['@id']:
            var_dict.update({'Broader_P07': id['@id']})
    # Related P06 matching (Units)
    for id in info[0]['http://www.w3.org/2004/02/skos/core#related']:
        if '/P06/' in id['@id']:
            var_dict.update({'Related_P06': id['@id']})
    # Related Sensors L22

    return var_dict

# Generate NERC Dataframe
# Retrieve all the  P01 data
p01 = get_nvs_variable_info(variable=None)
df_p01 = pd.DataFrame(p01)  # Convert to a dataframe

# Extract different info form nerc id, add them to the dataframe
df_var = df_p01['@id'].apply(split_nerc_id).apply(pd.Series).drop(['http','empty','unknown'],axis='columns')
df_p01 = df_p01.merge(df_var,left_index=True,right_index=True)

# For each variables, retrieve variable info
df_p01['variable_dict'] = df_p01['@id'].progress_apply(retrieve_variable_info)
# Convert dictionary to a columns
df_p01.merge(df_p01['variable_dict'].apply(pd.Series),left_index=True,right_index=True)

