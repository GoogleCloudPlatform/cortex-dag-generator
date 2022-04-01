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

import os
import sys
import yaml
import jinja2
import logging
from generate_query import *
from google.cloud.exceptions import NotFound
from pathlib import Path

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not sys.argv[1]:
    raise SystemExit('No Source Project provided')
source_project = sys.argv[1]

if not sys.argv[2]:
    raise SystemExit('No Source Dataset provided')
source_dataset = sys.argv[1] + '.' + sys.argv[2]

if not sys.argv[3]:
    raise SystemExit('No Target Project provided')
target_project = sys.argv[3]

if not sys.argv[4]:
    raise SystemExit('No Target Project/Dataset provided')
target_dataset = sys.argv[3] + '.' + sys.argv[4]

if not sys.argv[5]:
    raise SystemExit('No GCS bucket provided')
gcs_bucket = sys.argv[5]

if not sys.argv[6]:
    raise SystemExit('No Test flag provided')
gen_test = sys.argv[6]

if not sys.argv[7]:
    logging.info("SQL Flavour not provided. Defaulting to ECC")
    sql_flavour = "ECC"
sql_flavour = sys.argv[7]


os.makedirs("../generated_dag", exist_ok=True)
os.makedirs("../generated_sql", exist_ok=True)

# Process tables
with open('../setting.yaml') as tmp:

    t = jinja2.Template(tmp.read(), trim_blocks=True, lstrip_blocks=True)
    f = t.render({'sql_flavour': sql_flavour})

    tables_to_replicate = yaml.load(f, Loader=yaml.FullLoader)

    for table in tables_to_replicate['data_to_replicate']:
        logging.info(f"== Processing table {table['base_table']} ==")
        cdc_base_table = source_dataset + '.' + table['base_table']
        if not 'target_table' in table:
            table['target_table'] = table['base_table']
        cdc_target_table = target_dataset + '.' + table['target_table']

        keys = []
        keys = get_keys(source_dataset, table['base_table'])
        if not keys:
            e_msg = f"Keys for table {table['base_table']} not found in DD03L"
            logging.error(e_msg)
            raise SystemExit(e_msg)

        try:
            if table['load_frequency'] == "RUNTIME":
                logging.info(f"Generating view {cdc_target_table}")
                generate_runtime_sql(cdc_base_table, cdc_target_table, keys,
                                     source_project)
            else:
                try:
                    logging.info(f"Creating target table {cdc_base_table}")
                    check_create_target(cdc_base_table, cdc_target_table, gen_test)
                except NotFound:
                    logging.error(f"Table {cdc_target_table} not found")
                    raise SystemExit(f"Table {cdc_target_table} not found")

                logging.info(f"Generating dag for {cdc_base_table}")

                today = datetime.datetime.now()
                substitutes =   {
                    "base_table" : cdc_base_table,
                    "year" : today.year,
                    "month" : today.month, "day" : today.day,
                    "query_file" : "cdc_" + cdc_base_table.replace(".", "_") + ".sql",
                    "load_frequency" : table['load_frequency']
                }
                generate_dag(cdc_base_table, "template_dag/dag_sql.py", **substitutes)

                logging.info(f"Generating sql for {cdc_target_table}")
                generate_sql(cdc_base_table, cdc_target_table, keys,
                             source_project, gen_test)
        except Exception as e:
            logging.error(
                f"Error generating dag/sql from {table} error message {str(e)}"
            )
            raise SystemExit(
                'Error while generating sql and dags. Please check the logs')

# Move all files to customer's final GCS bucket - TBD move to script step with gsutil
# for filename in os.listdir('../generated_dag/'):
#     logging.info(f"Uploading {filename} to {gcs_bucket}/dags/")
#     copy_to_storage(gcs_bucket, "dags", '../generated_dag', filename)

# for filename in os.listdir('../generated_sql/'):
#     logging.info(f"Uploading {filename} to {gcs_bucket}/data/data_replication/")
#     copy_to_storage(gcs_bucket, "data/data_replication", '../generated_sql',
#                     filename)
