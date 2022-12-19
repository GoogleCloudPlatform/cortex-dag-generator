#!/bin/bash

# TODO: this will be read from deploy.yaml at a later phase
EXTERNAL_DAGS=("date_dimension" "currency_conversion" "holiday_calendar" "trends" "prod_hierarchy_texts" "weather")

#--------------------
# Help Message
#--------------------
usage() {
  cat <<HELP_USAGE

Generate exernal DAG files (weather, trends, etc.).

$0 [OPTIONS]

Options
-h | --help                       : Display this message
-a | --source-project             : Source Dataset Project ID. Mandatory
-x | --cdc-processed-dataset      : Source Dataset Name. Default: CDC_PROCESSED
-l | --location                   : BigQuery dataset location. Default US
-t | --test-data                  : Populate with test data. Default false
-s | --run-ext-sql                : Run external DAGs SQLs Default: true

HELP_USAGE

}

#--------------------
# Validate input
#--------------------
validate() {

  if [ -z "${project_id_src-}" ]; then
    echo 'ERROR: "source-project" is required. See help for details.'
    exit 1
  fi

  if [ -z "${run_ext_sql-}" ]; then
    echo 'INFO: External DAGs SQL files will be executed.'
    run_ext_sql="true"
  fi

  if [ -z "${test_data-}" ]; then
    echo 'INFO: test data will not be loaded.'
    test_data="false"
  fi

  if [ -z "${location-}" ]; then
    echo 'INFO: "location" not provided. Defaulting to US.'
    location="US"
  fi

  if [ -z "${dataset_cdc_processed-}" ]; then
    echo 'INFO: "cdc-processed-dataset" not provided, defaulting to CDC_PROCESSED.'
    dataset_cdc_processed="CDC_PROCESSED"

    exists=$(bq query --location="${location}" --project_id="${project_id_src}" --use_legacy_sql=false "select distinct 'KITTYCORN' from ${dataset_cdc_processed}.INFORMATION_SCHEMA.TABLES")
    if [[ ! "$exists" == *"KITTYCORN"* ]]; then
      echo "ERROR: Dataset $dataset_cdc_processed does not exist, aborting"
      exit 1
    fi
  fi

}

#--------------------
# Parameters parsing
#--------------------

set -o errexit -o noclobber -o nounset -o pipefail
params="$(getopt -o ha:x:l:t:s: -l help,source-project:,cdc-processed-dataset:,location:,test-data:,run-ext-sql: --name "$0" -- "$@")"
eval set -- "$params"

while true; do
  case "$1" in
    -h | --help)
      usage
      shift
      exit
      ;;
    -a | --source-project)
      project_id_src=$2
      shift 2
      ;;
    -x | --cdc-processed-dataset)
      dataset_cdc_processed=$2
      shift 2
      ;;
    -l | --location)
      location=$2
      shift 2
      ;;
    -t | --test-data)
      test_data="${2}"
      shift 2
      ;;
    -s | --run-ext-sql)
      run_ext_sql="${2}"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "Invalid option ($1). Run --help for usage" >&2
      exit 1
      ;;
  esac
done

set +o errexit +o noclobber +o nounset +o pipefail

#--------------------
# Main logic
#--------------------

validate

success=0

cat <<EOF >data.json
{
  "project_id_src": "${project_id_src}",
  "dataset_cdc_processed": "${dataset_cdc_processed}"
}
EOF

##
# assumption is that all files are in a folder with the same name as the deployment configuration
# e.g. "holiday" folder contains all holiday dag files etc
# the DAGS should feed and clean the data coming from the APIs - so there's technically no RAW_LANDING
##

mkdir -p generated_dag
mkdir -p generated_sql

lowcation=$(echo "${location}" | tr '[:upper:]' '[:lower:]')

## The bucket for australia-southeast1 was taken
if [[ "${lowcation}" == 'australia-southeast1' ]]; then
    lowcation=australia-southeast11
fi


for dag in "${EXTERNAL_DAGS[@]}"; do

  echo "INFO: checking for external DAG $dag"
  if [ -d "src/external_dag/${dag}" ]; then

    echo "INFO: Found, creating external DAG ${dag}"
    paths=(src/external_dag/"${dag}"/*)

    for p in "${paths[@]}"; do
      echo "INFO: processing file ${p}"
      file=$(basename "${p}")

      if [[ $p = *.py ]]; then
        cp "${p}" "generated_dag/${file}"
      fi

      if [[ $p = *.ini ]]; then
        jinja -d data.json "${p}" >"generated_dag/${file}"
        if [ $? = 1 ]; then success=1; fi
      fi

      if [[ $p = *.sql ]]; then
        query=$(jinja -d data.json "${p}")
        echo "${query}" >"generated_sql/${file}"
        if [[ "${run_ext_sql}" == "true" ]]
        then
          bq query --batch --project_id="${project_id_src}" --location="${location}" --use_legacy_sql=false "${query}"
          _sql_code=$?
        else
          echo "${file} will not be executed (--run-ext-sql is false)."
          _sql_code=1
        fi
        if [ $_sql_code -ne 0 ] && [[ "${test_data}" != "true" ]]; then
          if [[ "${run_ext_sql}" != "true" ]]
          then
            echo "${file} was not executed (--run-ext-sql is false) and --test-data is ${test_data}. This is unusual, but ok."
          else
            echo "ERROR: ${file} execution was not successful and --test-data is false."
            success=1
          fi
        else
          if [[ "${test_data}" == "true" ]]; then
            table_name="${file%.*}"
            echo "Processing test data for ${table_name}"
            num_rows_str=$(bq query --location="${location}" --project_id="${project_id_src}" \
              --use_legacy_sql=false --format=csv --quiet \
              "SELECT COUNT(*) FROM \`${dataset_cdc_processed}.${table_name}\`")
            if [[ $? -ne 0 ]]
            then
              num_rows=0
            else
              num_rows=$(echo -e "${num_rows_str}" | tail -1)
            fi
            if [ "$num_rows" -eq 0 ]; then
              parquet_file="gs://kittycorn-test-harness-${lowcation}/ext/${table_name}.parquet"
              echo "INFO: Loading test data for $table_name"
              bq load --location="${location}" --project_id "${project_id_src}" --noreplace --source_format=PARQUET "${dataset_cdc_processed}.${table_name}" "${parquet_file}"
              if [ $? = 1 ]; then success=1; fi
            else
              echo "INFO: Skipping loading of test data for $table_name as it already has data"
            fi
          fi
        fi
      fi
    done

  else
    echo "External dag ${dag} not found"
    success=1
  fi

done

exit "${success}"
