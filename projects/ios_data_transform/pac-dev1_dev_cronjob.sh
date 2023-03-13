#!/bin/bash

# Load environment variables
source ./.env

## some definitions$
DIRECTORY=`dirname $0`$

## setup python environment
source ~/anaconda3/etc/profile.d/conda.sh
conda activate cioos_data_transform310
python --version
echo `which python`

python ios_data_transform_script.py all drf -i /data/ios_raw_files_temp/www.waterproperties.ca/osd_data_archive/Drifter_Data/ -o /data/erddap_data/IOS_DRF_Data/
python ios_data_transform_script.py all tob -i /data/ios_raw_files_temp/www.waterproperties.ca/osd_data_archive/Cruise_Data/ -o /data/erddap_data/IOS_TOB_Data/
python ios_data_transform_script.py all ubc -i /data/ios_raw_files_temp/www.waterproperties.ca/osd_data_archive/UBC/ -o /data/erddap_data/IOS_UBC_Data/
python ios_data_transform_script.py all ane -i /data/ios_raw_files_temp/www.waterproperties.ca/osd_data_archive/Weather_Data/ -o /data/erddap_data/IOS_ANE_Data/

# python ios_data_transform_script.py all ctd -i /data/ios_raw_files_temp/www.waterproperties.ca/osd_data_archive/Cruise_Data/ -o /data/erddap_data/IOS_CTD_profiles_new/
# python ios_data_transform_script.py all bot -i /data/ios_raw_files_temp/www.waterproperties.ca/osd_data_archive/Cruise_Data/ -o /data/erddap_data/IOS_BOT_profiles_new/
# python ios_data_transform_script.py all cur -i /data/ios_raw_files_temp/www.waterproperties.ca/osd_data_archive/Mooring_Data/ -o /data/erddap_data/IOS_CUR_moorings_new/
