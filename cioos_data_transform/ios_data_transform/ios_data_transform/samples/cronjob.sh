#!/bin/bash
# some definitions$
DIRECTORY=`dirname $0`$
# setup python environment
__conda_setup="$('/home/ios/install/miniconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
eval "$__conda_setup"
echo "Finished setting up python environment"
echo "Change to working directory..."
cd /home/ios/
# run script to convert new ctd files
python --version
echo `which python`
python ios_data_transform_script.py new ctd > ~/ctd.log 2>&1
python ios_data_transform_script.py all mctd > ~/mctd.log 2>&1
python ios_data_transform_script.py all bot > ~/bot.log 2>&1


# set flag on ERDDAP to reload/rebuild dataset
#touch /home/hakai/erddap-ios/bigParentDirectory/hardFlag/IOS_CTD_test
wget -qO - --user cioos --password ***  "https://pac-dev1.cioos.org/erddap/setDatasetFlag.txt?datasetID=IOS_CTD_Profiles&flagKey=5754e1b637451fcbd8b0fc6e3741bacd02743ecfb533533c5c3fe98bacb0208c" >> ctd.log
wget -qO - --user cioos --password ***  "https://pac-dev1.cioos.org/erddap/setDatasetFlag.txt?datasetID=IOS_CTD_Moorings&flagKey=56c5d942e8aac6da23086410e978d5ca12c0cd2f796066f8496d615db1fba042" >> mctd.log
wget -qo - --user cioos --password ***  "https://pac-dev1.cioos.org/erddap/setDatasetFlag.txt?datasetID=IOS_BOT_Profiles&flagKey=da740e78f9344e340c8625a1a98c5a64747add5fa9faa322b2ba8fdeebf7aec4" >> bot.log
# mail someone with the results or with error message
mail -s "CIOOS message(ctd)" pramod.thupaki@hakai.org < ~/ctd.log
mail -s "CIOOS message(mctd)" pramod.thupaki@hakai.org < ~/mctd.log
mail -s "CIOOS message(bot)" pramod.thupaki@hakai.org < ~/bot.log

