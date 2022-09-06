# pylint:disable=unspecified-encoding consider-using-with

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
"""Useful functions to carry out neecssary operations."""

#TODO: Make file fully lintable, and remove all pylint disabled flags.
#TODO: Arrange functions in more logical order.

import datetime
from string import Template

from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage

_SQL_DAG_PYTHON_TEMPLATE = 'template_dag/dag_sql.py'
_SQL_DAG_SQL_TEMPLATE = 'template_sql/cdc_sql_template.sql'
_VIEW_SQL_TEMPLATE = 'template_sql/runtime_query_view.sql'
_HIER_DAG_PYTHON_TEMPLATE = 'template_dag/dag_sql_hierarchies.py'

_GENERATED_DAG_DIR = '../generated_dag'
_GENERATED_SQL_DIR = '../generated_sql'

# Columns to be ignored for CDC tables
_CDC_EXCLUDED_COLUMN_LIST = ['_PARTITIONTIME', 'operation_flag', 'is_deleted']

client = bigquery.Client()
storage_client = storage.Client()


def generate_dag_py_file(template, file_name, **dag_subs):
    """Generates DAG definition python file from template."""
    dag_template_file = open(template, 'r')
    dag_template = Template(dag_template_file.read())
    generated_dag_code = dag_template.substitute(**dag_subs)

    dag_file = _GENERATED_DAG_DIR + '/' + file_name
    generated_dag_file = open(dag_file, 'w+')
    generated_dag_file.write(generated_dag_code)
    generated_dag_file.close()
    print(f'Created DAG python file {dag_file}')


def generate_runtime_view(raw_table_name, cdc_table_name):
    """Creates runtime CDC view for RAW table."""

    keys = get_keys(raw_table_name)
    if not keys:
        e_msg = f'Keys for table {raw_table_name} not found in table DD03L'
        raise SystemExit(e_msg)

    keys_with_dt1_prefix = ','.join(add_prefix_to_keys('DT1', keys))
    keys_comparator_with_dt1_t1 = ' AND '.join(
        get_key_comparator(['DT1', 'T1'], keys))
    keys_comparator_with_t1_s1 = ' AND '.join(
        get_key_comparator(['T1', 'S1'], keys))
    keys_comparator_with_t1s1_d1 = ' AND '.join(
        get_key_comparator(['D1', 'T1S1'], keys))

    # Generate view sql by using template.
    sql_template_file = open(_VIEW_SQL_TEMPLATE, 'r')
    sql_template = Template(sql_template_file.read())
    generated_sql = sql_template.substitute(
        base_table=raw_table_name,
        keys=', '.join(keys),
        keys_with_dt1_prefix=keys_with_dt1_prefix,
        keys_comparator_with_t1_s1=keys_comparator_with_t1_s1,
        keys_comparator_with_dt1_t1=keys_comparator_with_dt1_t1,
        keys_comparator_with_t1s1_d1=keys_comparator_with_t1s1_d1)

    view = bigquery.Table(cdc_table_name)
    view.view_query = generated_sql
    client.create_table(view, exists_ok=True)
    print(f'Created view {cdc_table_name}')


def generate_cdc_dag_files(raw_table_name, cdc_table_name, load_frequency,
                           gen_test):
    """Generates file contaiing DAG code to refresh CDC table from RAW table.

    Args:
        table_name: name of the table for which DAG needs to be generated.
        **dag_subs: List of substitues to be made to the DAG template.
    """

    dag_file_name_part = 'cdc_' + raw_table_name.replace('.', '_')
    dag_py_file_name = dag_file_name_part + '.py'
    dag_sql_file_name = dag_file_name_part + '.sql'

    today = datetime.datetime.now()
    substitutes = {
        'base_table': raw_table_name,
        'year': today.year,
        'month': today.month,
        'day': today.day,
        'query_file': dag_sql_file_name,
        'load_frequency': load_frequency
    }

    # Create python DAG flie.
    generate_dag_py_file(_SQL_DAG_PYTHON_TEMPLATE, dag_py_file_name,
                         **substitutes)

    # Create query for SQL script that will be used in the DAG.
    fields = []
    update_fields = []

    raw_table_schema = client.get_table(raw_table_name).schema
    for field in raw_table_schema:
        if field.name not in _CDC_EXCLUDED_COLUMN_LIST:
            fields.append(f'`{field.name}`')
            update_fields.append((f'T.`{field.name}` = S.`{field.name}`'))

    if not fields:
        print(f'Schema could not be retrieved for {raw_table_name}')

    keys = get_keys(raw_table_name)
    if not keys:
        e_msg = f'Keys for table {raw_table_name} not found in table DD03L'
        raise SystemExit(e_msg)

    p_key_list = get_key_comparator(['S', 'T'], keys)
    p_key_list_for_sub_query = get_key_comparator(['S1', 'T1'], keys)
    p_key = ' AND '.join(p_key_list)
    p_key_sub_query = ' AND '.join(p_key_list_for_sub_query)

    sql_template_file = open(_SQL_DAG_SQL_TEMPLATE, 'r')
    sql_template = Template(sql_template_file.read())

    seperator = ', '

    generated_sql = sql_template.substitute(
        base_table=raw_table_name,
        target_table=cdc_table_name,
        p_key=p_key,
        fields=seperator.join(fields),
        update_fields=seperator.join(update_fields),
        keys=', '.join(keys),
        p_key_sub_query=p_key_sub_query)

    # Create sql file containing the query
    cdc_sql_file = _GENERATED_SQL_DIR + '/' + dag_sql_file_name
    generated_sql_file = open(cdc_sql_file, 'w+')
    generated_sql_file.write(generated_sql)
    generated_sql_file.close()
    print(f'Created DAG sql file {cdc_sql_file}')

    # Create view on top of CDC table.
    view_id = cdc_table_name + '_view'
    view = bigquery.Table(view_id)
    view.view_query = f'SELECT * EXCEPT (recordstamp) FROM `{cdc_table_name}`'
    view = client.create_table(view, exists_ok=True)
    print(f'Created view {view_id}')

    # If test data is needed, we want to populate CDC tables as well
    # from data in the RAW tables.
    # Good thing is - we already have the sql query available to do that.
    if gen_test.upper() == 'TRUE':
        print(f'Populating {cdc_table_name} table with data '
              f'from {raw_table_name} table')
        client.query(generated_sql)


def generate_hier_dag_files(file_name, **dag_subs):
    generate_dag_py_file(_HIER_DAG_PYTHON_TEMPLATE, file_name, **dag_subs)


def get_key_comparator(table_prefix, keys):
    p_key_list = []
    for key in keys:
        # pylint:disable=consider-using-f-string
        p_key_list.append('{0}.`{2}` = {1}.`{2}`'.format(
            table_prefix[0], table_prefix[1], key))
    return p_key_list


def get_comparator_with_select(table_name, keys):
    p_key_list = []
    for key in keys:
        p_key_list.append(
            f'`{key}` NOT IN (SELECT `{key}` FROM `{table_name}`)')
    return p_key_list


def add_prefix_to_keys(prefix, keys):
    prefix_keys = []
    for key in keys:
        prefix_keys.append(f'{prefix}.`{key}`')
    return prefix_keys


def create_cdc_table(raw_table_name, cdc_table_name):
    """Creates CDC table based on source RAW table schema.

    Retrives schema details from source table in RAW dataset and creates a
    table in CDC dataset based on that schema if it does not exist.

    Args:
        raw_table_name: Full table name of raw table (dataset.table_name).
        cdc_table_name: Full table name of cdc table (dataset.table_name).

    Raises:
        NotFound: Bigquery table not found.

    """

    try:
        client.get_table(cdc_table_name)
        print(f'Table {cdc_table_name} already exists.')
    except NotFound:
        raw_table_schema = client.get_table(raw_table_name).schema
        target_schema = [
            field for field in raw_table_schema
            if field.name not in _CDC_EXCLUDED_COLUMN_LIST
        ]

        cdc_table = bigquery.Table(cdc_table_name, schema=target_schema)
        client.create_table(cdc_table)

        print(f'Created table {cdc_table_name}.')


def check_create_hiertable(full_table, field):
    try:
        client.get_table(full_table)
    except NotFound:
        schema = [
            bigquery.SchemaField('mandt', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('parent', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('parent_org', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('child', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('child_org', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField(field, 'STRING', mode='NULLABLE')
        ]

        table = bigquery.Table(full_table, schema=schema)
        table = client.create_table(table)
        print(f'Created {full_table}')


def create_view(target_table_name, sql):
    if sql != '':
        view_id = target_table_name
        view = bigquery.Table(view_id)
        view.view_query = sql
    else:
        view_id = target_table_name + '_view'
        view = bigquery.Table(view_id)
        view.view_query = (
            f'SELECT * EXCEPT (recordstamp) FROM `{target_table_name}`')
    # Make an API request to create the view.
    view = client.create_table(view, exists_ok=True)
    print(f'Created {view.table_type}: {str(view.reference)}')


def get_keys(full_table_name):
    """Retrieves primary key columns for raw table from metadata table.

    Args:
        full_table_name: Full table name in project.dataset.table_name format.
    """

    _, dataset, table_name = full_table_name.split('.')
    query = (f'SELECT fieldname '
             f'FROM `{dataset}.dd03l` '
             f'WHERE KEYFLAG = "X" AND fieldname != ".INCLUDE" '
             f'AND tabname = "{table_name.upper()}"')
    query_job = client.query(query)

    fields = []
    for row in query_job:
        fields.append(row['fieldname'])
    return fields


def copy_to_storage(gcs_bucket, prefix, directory, filename):
    try:
        bucket = storage_client.get_bucket(gcs_bucket)
    except Exception as e:  #pylint:disable=broad-except
        print(f'Error when accessing GCS bucket: {gcs_bucket}')
        print(f'Error : {str(e)}')
    blob = bucket.blob(f'{prefix}/{filename}')
    blob.upload_from_filename(f'{directory}/{filename}')
