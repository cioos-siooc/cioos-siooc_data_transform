#!/bin/bash

# Load environment variables
source .env

## some definitions$
DIRECTORY=`dirname $0`$
## setup python environment
__conda_setup="$('/home/ios/install/miniconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
eval "$__conda_setup"
echo "Finished setting up python environment"
echo "Change to working directory..."
cd /home/ios/
## run script to convert new ctd files
python --version
echo `which python`
python ios_data_transform_script.py new ctd > ~/ctd.log 2>&1
python ios_data_transform_script.py new mctd > ~/mctd.log 2>&1
python ios_data_transform_script.py new bot > ~/bot.log 2>&1
python ios_data_transform_script.py new cur > ~/cur.log 2>&1


## set flag on ERDDAP to reload/rebuild dataset
## touch /home/hakai/erddap-ios/bigParentDirectory/hardFlag/IOS_CTD_test
wget -qO - --user ${PAC_DEV1_USER} --password ${PAC_DEV_PASSWORD}  "https://pac-dev1.cioos.org/erddap/setDatasetFlag.txt?datasetID=IOS_CTD_Profiles&flagKey=5754e1b637451fcbd8b0fc6e3741bacd02743ecfb533533c5c3fe98bacb0208c" >> ctd.log
wget -qO - --user ${PAC_DEV1_USER} --password ${PAC_DEV_PASSWORD}  "https://pac-dev1.cioos.org/erddap/setDatasetFlag.txt?datasetID=IOS_CTD_Moorings&flagKey=56c5d942e8aac6da23086410e978d5ca12c0cd2f796066f8496d615db1fba042" >> mctd.log
wget -qO - --user ${PAC_DEV1_USER} --password ${PAC_DEV_PASSWORD}  "https://pac-dev1.cioos.org/erddap/setDatasetFlag.txt?datasetID=IOS_BOT_Profiles&flagKey=da740e78f9344e340c8625a1a98c5a64747add5fa9faa322b2ba8fdeebf7aec4" >> bot.log
## mail someone with the results or with error message
mail -s "CIOOS message(ctd)" ${REPORTS_EMAIL_RECEIVERS} < ~/ctd.log
mail -s "CIOOS message(mctd)" ${REPORTS_EMAIL_RECEIVERS} < ~/mctd.log
mail -s "CIOOS message(bot)" ${REPORTS_EMAIL_RECEIVERS} < ~/bot.log
mail -s "CIOOS message(cur)" ${REPORTS_EMAIL_RECEIVERS} < ~/cur.log

## rsync adcp netcdf files from ios_raw_files to /data/erddap_data/
chmod a-x /home/ios/ios_raw_files/adcp_data/*.nc
rsync -r --delete /home/ios/ios_raw_files/adcp_data/*.nc /data/erddap_data/IOS_ADCP_moorings/

chmod 644 /data/erddap_data/IOS_ADCP_moorings/*.nc
wget -q0 - --user ${PAC_DEV1_USER} --password ${PAC_DEV_PASSWORD}  "https://pac-dev1.cioos.org/erddap/setDatasetFlag.txt?datasetID=IOS_ADCP_Moorings&flagKey=6ab516ecc16524bea0f4efa8c791b297fe95da9f1da35f8001459c3e18a5a0cd"