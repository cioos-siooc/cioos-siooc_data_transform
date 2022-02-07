import pandas as pd
import logging
import numpy as np
from sqlalchemy import column

from tqdm import tqdm
import requests

tqdm.pandas()

import re
import os

logging.basicConfig()
logger = logging.getLogger(__name__)


# Convert to UDUNIT
def get_udunits_from_erddap(units):
    """Use ERDDAP standardize UDUNIT method to try to standardize any units"""
    if units in [None, np.nan, ""]:
        return None
    response = requests.get(
        f"https://catalogue.hakai.org/erddap/convert/units.txt?STANDARDIZE_UDUNITS={units}"
    )
    return response.text


print("Load ODF Reference Vocabulary")
vocab_path = "projects\odf_transform\odf_transform\ODF_Reference_Vocabulary.csv"
df_vocab = pd.read_csv(vocab_path)
df_vocab.to_csv(vocab_path + "_original.csv")
# print("Convert ODF vocab units to UDUNITS")
# df_vocab["udunits"] = df_vocab["units"].progress_apply(get_udunits_from_erddap)

p07_path = "projects\odf_transform\odf_transform\p07_reference.csv"
if os.path.exists(p07_path):
    print("Retrieve CF P07 terms locally")
    df_P07 = pd.read_csv(p07_path)
else:
    print("Retrieve CF P07 terms from NERC")
    df_P07 = pd.read_xml(
        "https://vocab.nerc.ac.uk/collection/P07/current/?_profile=nvs&_mediatype=application/rdf+xml"
    )
    df_P07.to_csv(p07_path)
standard_name_list = df_P07["prefLabel"].tolist()


# p06_path = "projects\odf_transform\odf_transform\p06_reference.csv"
# if os.path.exists(p06_path):
#     print("Retrieve Units P06 locally")
#     df_P06 = pd.read_csv(p06_path)
# else:
#     print("Retrieve Units P06 from NERC")
#     df_P06 = pd.read_xml(
#         "https://vocab.nerc.ac.uk/collection/P06/current/?_profile=nvs&_mediatype=application/rdf+xml"
#     )
#     print("Convert ODF units to UDUNITS")
#     df_P06["udunits"] = df_P06["prefLabel"].progress_apply(get_udunits_from_erddap)
#     df_P06["udunits2"] = df_P06["altLabel"].progress_apply(get_udunits_from_erddap)
#     df_P06.dropna(subset=["udunits"])
#     df_P06.to_csv(p06_path)

for id, row in tqdm(df_vocab.iterrows(), total=df_vocab.shape[0]):
    # Review standard_name
    if (
        row["standard_name"] not in [None, np.nan]
        and row["standard_name"] not in standard_name_list
    ):
        logger.warn(f"Drop {row['standard_name']} does not exist.")
        df_vocab.loc[id, "standard_name"] = None

    # Review BODC P01
    if row["sdn_parameter_urn"] not in [None, np.nan]:
        nvs_item = re.search("SDN:(?P<vocab>.+)::(?P<urn>.+)", row["sdn_parameter_urn"])
        try:
            df_nvs_item = pd.read_xml(
                f"https://vocab.nerc.ac.uk/collection/{nvs_item['vocab']}/current/{nvs_item['urn']}/?_profile=nvs&_mediatype=application/rdf+xml"
            )
        except:
            logger.warn(f"NERC Term: {row['sdn_parameter_urn']} does not exist")
            continue

        if not df_nvs_item.empty:
            df_vocab.loc[id, "sdn_parameter_name"] = df_nvs_item["prefLabel"][0]
            df_vocab.loc[id, "SDN:P01::Alternative Name"] = df_nvs_item["altLabel"][0]
        else:
            logger.warn(f"Unknown NERC Term {row['sdn_parameter_urn']}")

    # # Review units and find a potential match in P06
    # if row["units"] not in [None, "", np.nan]:
    #     matched_udunits = df_P06.query(
    #         f"udunits =='{row['udunits']}' or udunits2 =='{row['udunits']}'"
    #     )
    #     if matched_udunits.empty:
    #         logger.warn(f"Can't any matching units in P06 for {row['units']}")
    #     else:
    #         df_vocab.loc[id,"sdn_uom_urn"] = matched_udunits["identifier"].iloc[0]
    #         df_vocab.loc[id,"sdn_uom_name"] = matched_udunits["prefLabel"].iloc[0]

# Overwrite original vocabulary
# df_vocab.drop(columns=["udunits"], inplace=True)
df_vocab.to_csv(
    "projects\odf_transform\odf_transform\ODF_Reference_Vocabulary_reviewed.csv",
    index=False,
)

