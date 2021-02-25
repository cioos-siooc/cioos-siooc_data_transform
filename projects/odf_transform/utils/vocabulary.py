import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import xmltodict
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


def get_cf_names():
    response = requests.get('https://cfconventions.org/Data/cf-standard-names/77/src/cf-standard-name-table.xml',
                            stream=True)
    response.raw.decode_content = True
    cf_dict = xmltodict.parse(response.text)

    # Convert to dataframes
    df_cf_alias = pd.DataFrame(cf_dict['standard_name_table']['alias'])
    df_cf = pd.DataFrame(cf_dict['standard_name_table']['entry'])
    return df_cf, df_cf_alias

def retrieve_variable_info(nerc_id=None,
                           variable=None,
                           vocabulary=None,
                           nvs_url="http://vocab.nerc.ac.uk/collection/",
                           version="current"):
    """
    Method used to extract important information from the variable specific info page.
    """
    # if Variable is given instead
    if variable and vocabulary:
        nerc_id = '%s/%s/$%s' % (nvs_url, vocabulary, variable)

    info = get_nvs_variable_info(nerc_id)

    if len(info) > 1:
        assert RuntimeWarning('mutile info exists')

    var_dict = {}
    # Get Definition
    if 'http://www.w3.org/2004/02/skos/core#definition' in info[0]:
        for definition in info[0]['http://www.w3.org/2004/02/skos/core#definition']:
            var_dict.update({'definition_' + definition['@language']: definition['@value']})
    # Pref Label
    if 'http://www.w3.org/2004/02/skos/core#prefLabel' in info[0]:
        for prefLabel in info[0]['http://www.w3.org/2004/02/skos/core#prefLabel']:
            var_dict.update({'prefLabel_' + prefLabel['@language']: prefLabel['@value']})
    # Broader P07 matching (CF Name)
    if 'http://www.w3.org/2004/02/skos/core#broader' in info[0]:
        for id in info[0]['http://www.w3.org/2004/02/skos/core#broader']:
            if '/P07/' in id['@id']:
                var_dict.update({'Broader_P07': id['@id']})
    # Related P06 matching (Units)
    if 'http://www.w3.org/2004/02/skos/core#related' in info[0]:
        for id in info[0]['http://www.w3.org/2004/02/skos/core#related']:
            if '/P06/' in id['@id']:
                var_dict.update({'Related_P06': id['@id']})
    # Related Sensors L22

    return var_dict


def get_bio_reference(xlsx_path):
    df_bio = pd.read_excel(bio_list)
    df_bio.rename({'GF3(BIO) code': 'GF3'})
    df_bio['Organization'] = 'BIO'
    return df_bio


def read_dfo_variable_sheets():
    # Format BIO and MEDS variable lists
    df_meds = pd.read_csv('../vocabulary/meds_pcodes_20191212_mods_utf8.csv') \
        .rename({'CODE': 'GF3 CODE'}, axis='columns') \
        .dropna(how='all', axis='index')
    df_bio = pd.read_excel('bio_gf3_p01_mapping_2.7.2.xlsx') \
        .rename({'GF3(BIO) code': 'GF3 CODE'}, axis='columns') \
        .dropna(how='all', axis='index')
    df_bio['OWNER'] = 'BIO'  # Add bio as own to bio list

    # Merge the two together based on the owner and code
    df_dfo = pd.merge(df_meds, df_bio, how='outer', on=['OWNER', 'GF3 CODE'])
    return df_dfo


# Generate NERC Dataframe
# Retrieve all the  P01 data
print('Get P01 vocabulary')
p01 = get_nvs_variable_info(variable=None, vocabulary='P01')
df_p01 = pd.DataFrame(p01)  # Convert to a dataframe

# Extract different info form nerc id, add them to the dataframe
df_var = df_p01['@id'].apply(split_nerc_id).apply(pd.Series).drop(['http', 'empty', 'unknown'], axis='columns')
df_p01 = df_p01.merge(df_var, left_index=True, right_index=True)

# For each variables, retrieve variable info
df_p01['variable_dict'] = df_p01['@id'].progress_apply(retrieve_variable_info)
# Convert dictionary to a columns
df_p01.merge(df_p01['variable_dict'].apply(pd.Series), left_index=True, right_index=True)

# Save to csv
df_p01.to_csv('nerc_p01_list.csv')
