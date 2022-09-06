# pylint: disable=logging-fstring-interpolation

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
#
"""Module to create CDC tables, related DAGS and hierarchy datasets."""

# TODO: Make file fully lintable, and remove all pylint disabled flags.

import os
import sys
import yaml
import jinja2
import logging

from generate_query import generate_runtime_view
from generate_query import create_cdc_table
from generate_query import generate_cdc_dag_files

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not sys.argv[1]:
    raise SystemExit("No Source Project provided")
source_project = sys.argv[1]

if not sys.argv[2]:
    raise SystemExit("No Source Dataset provided")
source_dataset = sys.argv[1] + "." + sys.argv[2]

if not sys.argv[3]:
    raise SystemExit("No Target Project provided")
target_project = sys.argv[3]

if not sys.argv[4]:
    raise SystemExit("No Target Project/Dataset provided")
target_dataset = sys.argv[3] + "." + sys.argv[4]

if not sys.argv[5]:
    raise SystemExit("No Test flag provided")
gen_test = sys.argv[5]

if not sys.argv[6]:
    logging.info("SQL Flavour not provided. Defaulting to ECC")
    sql_flavour = "ECC"
sql_flavour = sys.argv[6]

os.makedirs("../generated_dag", exist_ok=True)
os.makedirs("../generated_sql", exist_ok=True)

# Process tables
with open("../setting.yaml", encoding="utf-8") as settings_file:

    t = jinja2.Template(
        settings_file.read(), trim_blocks=True, lstrip_blocks=True
    )
    resolved_settings = t.render({"sql_flavour": sql_flavour})

    tables_to_replicate = yaml.load(resolved_settings, Loader=yaml.SafeLoader)

    for table in tables_to_replicate["data_to_replicate"]:

        table_name = table["base_table"]
        load_frequency = table["load_frequency"]

        logging.info(f"== Processing table {table_name} ==")

        raw_table = source_dataset + "." + table_name

        if "target_table" in table:
            target_table = table["target_table"]
        else:
            target_table = table_name

        cdc_table = target_dataset + "." + target_table

        try:
            if load_frequency == "RUNTIME":
                logging.info(f"Generating view {cdc_table}")
                generate_runtime_view(raw_table, cdc_table)
            else:
                logging.info(f"Creating table {cdc_table}")
                create_cdc_table(raw_table, cdc_table)

                # Create files (python and sql) that will be used later to
                # create DAG in GCP that will refresh CDC tables from RAW
                # tables.
                logging.info("Generating required files for DAG")
                generate_cdc_dag_files(
                    raw_table, cdc_table, load_frequency, gen_test
                )

        except Exception as e:
            logging.error(
                f"Error generating dag/sql for {table_name}.\nError : {str(e)}"
            )
            raise SystemExit(
                "Error while generating sql and dags. Please check the logs"
            ) from e
