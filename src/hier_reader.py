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
import yaml
import sys
import logging
import os
import datetime
from dag_hierarchies_module import insert_rows, generate_hier
from generate_query import check_create_hiertable, generate_dag, copy_to_storage
from google.cloud.exceptions import NotFound
from pathlib import Path
from google.cloud import bigquery

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

os.makedirs("../generated_dag", exist_ok=True)
os.makedirs("../generated_sql", exist_ok=True)

client = bigquery.Client()

# Process hierarchies
with open('../sets.yaml') as f:
    datasets = yaml.load(f, Loader=yaml.SafeLoader)

    for set in datasets['sets_data']:
        logging.info(f"== Processing set {set['setname']} ==")
        nodes = []

        full_table = "{tgtd}.{tab}_hier".format(tgtd=target_dataset, tab=set['table'])

        # nodes = get_nodes(source_dataset, set['mandt'], set['setname'],
        #                 set['setclass'], set['orgunit'], set['table'], set['key_field'],
        #                 set['where_clause'], full_table)

        query = """SELECT  1
             FROM `{src_dataset}.setnode`
             WHERE setname = \'{setname}\' 
               AND setclass = \'{setclass}\'
               AND subclass = \'{org_unit}\' 
               AND mandt = \'{mandt}\' 
               LIMIT 1 """.format(src_dataset=source_dataset,
                                  setname=set['setname'],
                                  mandt=set['mandt'],
                                  setclass=set['setclass'],
                                  org_unit=set['orgunit'])
        query_job = client.query(query)
        print(query_job)
        if not query_job:
            logging.info("Dataset {s} not found in SETNODES".format(s=set['setname']))
            continue

        # Check if table exists, create it if not and populate with full initial load
        try:
            check_create_hiertable(full_table, set['key_field'])
            # insert_rows(full_table, nodes)

            logging.info("Generating dag for {ft}".format(ft=full_table))
            today = datetime.datetime.now()
            substitutes = {
                "setname": set['setname'],
                "full_table": full_table,
                "year": today.year,
                "month": today.month,
                "day": today.day,
                "src_project": source_project,
                "src_dataset": source_dataset,
                "setclass": set['setclass'],
                "orgunit": set['orgunit'],
                "mandt": set['mandt'],
                "table": set['table'],
                "select_key": set['key_field'],
                "where_clause": set['where_clause'],
                "load_frequency": set['load_frequency']
            }

            generate_dag(full_table, "template_dag/dag_sql_hierarchies.py", **substitutes)
            generate_hier(**substitutes)

        except NotFound:
            logging.error("Table {full_table} not found".format(full_table=full_table))
            raise SystemExit("Table {full_table} not found".format(full_table=full_table))

        # Copy template python processor used by all into specific directory
        copy_to_storage(gcs_bucket, "dags/hierarchies/", "./", "dag_hierarchies_module.py")
        # copy_to_storage(gcs_bucket, "dags/hierarchies/", "./template_dag", "__init.py__")
        # copy_to_storage(gcs_bucket, "dags/hierarchies/", "./template_dag", ".airflowignore")
