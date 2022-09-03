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

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
# from __future__ import print_function

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
## This DAG creates two table: currency_conversion for storing the exchange rate and other columns 
# and currency_decimal to to fix the decimal place of amounts for non-decimal-based currencies.
with DAG(
        'currency_conversion',
        template_searchpath=['/home/airflow/gcs/data/bq_data_replication/'],
        default_args=default_args,
        schedule_interval='@daily',
        start_date=datetime(2022, 8, 11),
) as dag:
  start_task = DummyOperator(task_id='start')
  currency_conversion = BigQueryOperator(
        task_id='currency_conversion',
        sql='currency_conversion.sql',
        create_disposition='WRITE_TRUNCATE', #This is one of the opions discussed, We will be making it as incremental load.
        bigquery_conn_id='sap_cdc_bq',
        use_legacy_sql=False)
  currency_decimal=BigQueryOperator(
        task_id='currency_decimal',
        sql='currency_decimal.sql',
        create_disposition='WRITE_TRUNCATE', #This is one of the opions discussed, We will be making it as incremental load.
        bigquery_conn_id='sap_cdc_bq',
        use_legacy_sql=False)
  stop_task = DummyOperator(task_id='stop')

  start_task >> currency_conversion >> currency_decimal >> stop_task