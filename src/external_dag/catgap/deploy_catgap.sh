#!/bin/bash

# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

CONFIG_FILE="config/config.json"
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DF_CONFIG_FILE="${SCRIPT_DIR}/../../../../../../${CONFIG_FILE}"

if [[ "$1" == "" || "$2" == "" ]]
then
    echo "ERROR: Target bucket or Logs bucket parameter is missing."
    echo "USAGE: deploy_catgap.sh TGT_BUCKET LOGS_BUCKET"
    exit 1
fi

if [[ ! -f "${DF_CONFIG_FILE}" ]]
then
    echo "ERROR: ${DF_CONFIG_FILE} not found."
    exit 1
fi

export source_project=$(cat ${DF_CONFIG_FILE} | python3 -c "import json,sys; print(str(json.load(sys.stdin)['projectIdSource']))" 2>/dev/null || echo "")
if [[ "${source_project}" == "" ]]
then
    echo "ERROR: projectIdSource value in ${CONFIG_FILE} is empty."
    exit 1
fi

target_bucket=$1
logs_bucket=$2

echo "Deploying CATGAP"
echo "Target Bucket: gs://$1"
echo "Logs Bucket: gs://$2"

cp -f "${DF_CONFIG_FILE}" "${SCRIPT_DIR}/config/config.json"
set +e
gcloud builds submit --project="${source_project}" \
    --config="${SCRIPT_DIR}/cloudbuild.catgap.yaml" \
    --substitutions \
    _TGT_BUCKET="${target_bucket}",_GCS_BUCKET="${logs_bucket}" \
    "${SCRIPT_DIR}"
_err=$?
rm -f "${SCRIPT_DIR}/config/config.json"

if [ $_err -ne 0 ]
then
    echo "CATGAP deployment failed."
    exit 1
fi

echo "CATGAP has been deployed."
