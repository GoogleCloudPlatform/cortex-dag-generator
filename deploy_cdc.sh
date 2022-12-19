#!/bin/bash

# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

project_id_src=$1
dataset_repl=$2
project_id_tgt=$3
dataset_tgt=$4
tgt_bucket=$5
log_bucket=$6
gen_test=$7
sql_flavour=$8
gen_external_data=$9
echo "Deploying CDC and unfolding hierarchies"

if [[ "${log_bucket}" == "" ]]
then
    # GCS_BUCKET in Data Foundation sap_config.env is the log bucket
    if [[ "${GCS_BUCKET}" != "" ]]
    then
        export log_bucket="${GCS_BUCKET}"
    else
        echo "No Build Logs Bucket name provided."
        cloud_build_project=$(gcloud config list --format 'value(core.project)' 2>/dev/null)
        export _GCS_LOG_BUCKET="${cloud_build_project}_cloudbuild"
        export log_bucket="${_GCS_LOG_BUCKET}"
        echo "Using ${_GCS_LOG_BUCKET}"
    fi
fi

#"Source" in this context is where data is replicated and "Target" is where the CDC results are peristed
gcloud builds submit --config=cloudbuild.cdc.yaml --substitutions=_PJID_SRC="$project_id_src",_DS_RAW="$dataset_repl",_PJID_TGT="$project_id_tgt",_DS_CDC="$dataset_tgt",_GCS_BUCKET="$tgt_bucket",_GCS_LOG_BUCKET="$log_bucket",_TEST_DATA="$gen_test",_SQL_FLAVOUR="$sql_flavour",_GEN_EXT="$gen_external_data" .


