#!/bin/bash

# Load environment variables
# shellcheck source=./.env
source .env

## setup python environment
# shellcheck source=/dev/null
source ~/anaconda3/etc/profile.d/conda.sh
conda activate odpy
python --version

odpy --log-file odpy-ios-drf-conversion.log --log-file-level=WARNING  convert -c odpy-ios-drf-conversion.yaml
odpy --log-file odpy-ios-tob-conversion.log --log-file-level=WARNING  convert -c odpy-ios-tob-conversion.yaml
odpy --log-file odpy-ios-ubc-conversion.log --log-file-level=WARNING  convert -c odpy-ios-ubc-conversion.yaml

odpy --log-file odpy-ios-bot-conversion.log --log-file-level=WARNING  convert -c odpy-ios-bot-conversion.yaml
odpy --log-file odpy-ios-ctd-conversion.log --log-file-level=WARNING  convert -c odpy-ios-ctd-conversion.yaml
odpy --log-file odpy-ios-mctd-conversion.log --log-file-level=WARNING  convert -c odpy-ios-mctd-conversion.yaml
odpy --log-file odpy-ios-cur-conversion.log --log-file-level=WARNING  convert -c odpy-ios-cur-conversion.yaml
