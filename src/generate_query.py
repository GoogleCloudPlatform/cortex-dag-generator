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
import argparse
import os
import datetime
import os
import json
from string import Template
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage
from pathlib import Path

client = bigquery.Client()
storage_client = storage.Client()


def check_current_column(colum, column_list):
    search_list = [column for column in column_list if column['field_name'].upper(
    ) == colum.upper()]
    if len(search_list) > 0:
        found_item = search_list[0]
        if found_item['datatype'] == 'DATE' or found_item['datatype'] == 'TIME':
            return (True, found_item['datatype'])
        else:
            return (False, found_item['datatype'])
    else:
        return (False, '')


def get_bq_schema(base_table):
    column_list = []
    results = client.get_table(base_table)
    base_schema = results.schema
    for column in base_schema:
        column = column.to_api_repr()
        column_list.append(
            {'field_name': column['name'], 'datatype': column['type']})

    return column_list


def table_columns(bq_table, source_project):
    bq_table_split = bq_table.split(".")
    query = "SELECT `column_name` FROM `{src_project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = \"{table_name}\"".format(
        dataset=bq_table_split[1],
        table_name=bq_table_split[2],
        src_project=source_project)
    query_job = client.query(query)  # Make an API request.
    seperator = ", "
    update_fields = []
    columns_to_exclude = ['_PARTITIONTIME', 'operation_flag', 'is_deleted']
    fields = []
    for row in query_job:
        column = row["column_name"]
        if column not in columns_to_exclude:
            fields.append(f"`{column}`")
            update_fields.append(f"T.`{column}` = S.`{column}`")
    return seperator.join(fields), seperator.join(update_fields)


def generate_dag(cdc_base_table, template, **subs):

    dag_template_file = open(template, "r")
    dag_template = Template(dag_template_file.read())
    generated_dag_template = dag_template.substitute(**subs)
    cdc_dag_filename = "cdc_" + cdc_base_table.replace(".", "_") + ".py"
    cdc_dag_filepath = "../generated_dag/" + cdc_dag_filename
    generated_dag_file = open(cdc_dag_filepath, "w+")
    generated_dag_file.write(generated_dag_template)
    generated_dag_file.close()


def generate_runtime_sql(cdc_base_table, cdc_target_view, keys, source_project):
    fields, update_fields = table_columns(cdc_base_table, source_project)
    if not fields:
        print(f"Schema could not be retrieved for {cdc_base_table}")
    keys_with_dt1_prefix = ",".join(add_prefix_to_keys('DT1', keys))
    keys_comparator_with_dt1_t1 = " AND ".join(
        get_key_comparator(['DT1', 'T1'], keys))
    keys_comparator_with_t1_s1 = " AND ".join(
        get_key_comparator(['T1', 'S1'], keys))
    keys_comparator_with_t1s1_d1 = " AND ".join(
        get_key_comparator(['D1', 'T1S1'], keys))
    sql_template_file = open(
        os.path.dirname(os.path.abspath(__file__)) +
        "/template_sql/runtime_query_view.sql", "r")
    sql_template = Template(sql_template_file.read())
    generated_sql_template = sql_template.substitute(
        base_table=cdc_base_table,
        keys=", ".join(keys),
        keys_with_dt1_prefix=keys_with_dt1_prefix,
        keys_comparator_with_t1_s1=keys_comparator_with_t1_s1,
        keys_comparator_with_dt1_t1=keys_comparator_with_dt1_t1,
        keys_comparator_with_t1s1_d1=keys_comparator_with_t1s1_d1)
    create_view(cdc_target_view, generated_sql_template)


def update_column_select(row, current_schema_list):
    field_name = row['fieldname']

    if '/' in row['fieldname']:
        field_name = row['fieldname'][1:].replace('/', '_')

    output = check_current_column(row['fieldname'], current_schema_list)

    if row['datatype'] == "DATS":
        if output[0]:
            return field_name
        else:
            field_name = f'IF({field_name} = "00000000", null, PARSE_DATE("%Y%m%d",SUBSTR(CAST({field_name} AS STRING),1,8))) AS {field_name}'
            return field_name
    elif row['datatype'] == "TIMS":
        if output[0]:
            return field_name
        else:
            field_name = f'IF({field_name} = "000000", null, PARSE_TIME("%H%M%S",SUBSTR(CAST({field_name} AS STRING),1,8))) AS {field_name}'
            return field_name
    else:
        return field_name


def get_dd03l_datatypes(base_table):
    # Instead of 'copy_table' operation perform the below:
    bq_table_split = base_table.split('.')
    # Get specific SAP datatype from DD03L table
    query = f"SELECT DISTINCT fieldname, tabname, datatype FROM `{bq_table_split[0]}.{bq_table_split[1]}.dd03l` \
            where tabname = '{bq_table_split[2].upper()}' and fieldname not like '.%'"
    query_job = client.query(query)
    date_list = []
    time_list = []
    column_list = []

    # Iterate through results (There should only be one as we are searching by table AND field)
    for row in query_job.result():
        column_list.append({'fieldname': row.fieldname,
                           'datatype': row.datatype, 'tabname': row.tabname})
        if row.datatype == "DATS":
            date_list.append(row.fieldname)
        if row.datatype == "TIMS":
            time_list.append(row.fieldname)

    return (date_list, time_list, column_list)


def generate_select_statement(dd03l_list, current_schema_list):
    field_data_list = dd03l_list[2]  # Gets column_list from DD03L table

    select_statement = ""

    # Iterate through results (There should only be one as we are searching by table AND field)
    for index, row in enumerate(field_data_list, 1):

        field_name = update_column_select(row, current_schema_list)

        if index == 1:
            select_statement = f"{field_name}"
        else:
            select_statement = f"{select_statement}, {field_name}"

    select_statement = f"{select_statement}, operation_flag, is_deleted, recordstamp"

    return select_statement


def get_data_types(bucket_name):
    bucket = storage_client.get_bucket(bucket_name)

    blob = storage.Blob(
        name='reference_data/sap_datatypes_to_bq.json',
        bucket=bucket,
    )
    sap_datatype_to_bq = json.loads(
        blob.download_as_string().decode('utf-8'))

    return sap_datatype_to_bq


def get_table_data(bucket_name, table_name):
    current_table_name = table_name.split('.')[2]
    bucket = storage_client.get_bucket(bucket_name)
    blob = storage.Blob(
        name='reference_data/partitioning_clusters.json',
        bucket=bucket,
    )
    partition_and_cluster_data = json.loads(
        blob.download_as_string().decode('utf-8'))
    output_table_data = [table_data for table_data in partition_and_cluster_data if table_data['table_name'].upper()
                         == current_table_name.upper()]
    print(f'Output Data: {output_table_data}')
    if len(output_table_data) > 0:
        output_table_data = output_table_data[0]
    return output_table_data


def generate_sql(cdc_base_table, cdc_target_table, keys, gcs_bucket, source_project, gen_test):

    fields, update_fields = table_columns(cdc_base_table, source_project)
    if not fields:
        print(f"Schema could not be retrieved for {cdc_base_table}")

    current_schema_list = get_bq_schema(cdc_base_table)
    dd03l_datatypes = get_dd03l_datatypes(cdc_base_table)
    select_statement = generate_select_statement(
        dd03l_datatypes, current_schema_list)

    p_key_list = get_key_comparator(
        ['S', 'T'], keys, dd03l_datatypes, current_schema_list)

    p_key_list_for_sub_query = get_key_comparator(
        ['S1', 'T1'], keys, dd03l_datatypes, current_schema_list)

    p_key = " AND ".join(p_key_list)

    p_key_sub_query = " AND ".join(p_key_list_for_sub_query)

    try:
        sql_template_file = open(
            os.path.dirname(os.path.abspath(__file__)) +
            "/template_sql/cdc_sql_template.sql", "r")
        sql_template = Template(sql_template_file.read())

        generated_sql_template = sql_template.substitute(
            base_table=cdc_base_table,
            target_table=cdc_target_table,
            select_statment=select_statement,
            p_key=p_key,
            fields=fields,
            update_fields=update_fields,
            keys=", ".join(keys),
            p_key_sub_query=p_key_sub_query)
        cdc_sql_filename = "cdc_" + cdc_base_table.replace(".", "_") + ".sql"
        cdc_sql_filepath = "generated_sql/" + cdc_sql_filename
        generated_sql_file = open(
            Path.joinpath(
                Path(__file__).resolve().parents[1], cdc_sql_filepath), "w")
        generated_sql_file.write(generated_sql_template)
        generated_sql_file.close()
    except:
        print(f"TECHNICAL Error - Unable to open SQL template or generate SQL")
        raise NotFound("SQL file")

    try:
        check_create_target(
            cdc_base_table, cdc_target_table, gcs_bucket, gen_test)
    except NotFound:
        print(
            f"Table {cdc_base_table} not replicated, not found or wrong dataset"
        )
        raise NotFound(cdc_base_table)
    sql = ""
    create_view(cdc_target_table, sql)


def generate_dag_hier(cdc_base_table, frequency):
    today = datetime.datetime.now()
    try:
        dag_template_file = open("template_dag/dag_sql_hierarchies.py", "r")
        dag_template = Template(dag_template_file.read())
        generated_dag_template = dag_template.substitute(
            base_table=cdc_base_table,
            year=today.year,
            month=today.month,
            day=today.day,
            query_file="cdc_" + cdc_base_table.replace(".", "_") + "_hier.sql",
            load_frequency=frequency)
        cdc_dag_filename = "cdc_" + \
            cdc_base_table.replace(".", "_") + "_hier.py"
        cdc_dag_filepath = "../generated_dag/" + cdc_dag_filename
        generated_dag_file = open(cdc_dag_filepath, "w+")
        generated_dag_file.write(generated_dag_template)
        generated_dag_file.close()
    except:
        print(f"TECHNICAL Error accessing template or generating DAG file")
        raise NotFound("DAG file")


def get_key_comparator(table_prefix, keys, dd03l_list, current_schema_list):
    p_key_list = []
    date_list = dd03l_list[0]  # Gets Date_list from DD03L function

    for key in keys:
        if key in date_list:
            output = check_current_column(key, current_schema_list)
            if output[0]:
                p_key_list.append(
                    f"{table_prefix[0]}.{key} = {table_prefix[1]}.{key}")
            else:
                p_key_list.append(
                    f"{table_prefix[0]}.`{key}` = PARSE_DATE('%Y%m%d',SUBSTR(CAST({table_prefix[1]}.`{key}` AS STRING),1,8))")
        else:
            p_key_list.append("{0}.`{2}` = {1}.`{2}`".format(
                table_prefix[0], table_prefix[1], key))
    return p_key_list


def get_comparator_with_select(table_name, keys):
    p_key_list = []
    for key in keys:
        p_key_list.append(
            f"`{key}` NOT IN (SELECT `{key}` FROM `{table_name}`)")
    return p_key_list


def add_prefix_to_keys(prefix, keys):
    prefix_keys = []
    for key in keys:
        prefix_keys.append("{0}.`{1}`".format(prefix, key))
    return prefix_keys


def check_create_target(base_table, target_table, gcs_bucket, gen_test):
    # If the base table hasn't been replicated, the exception will be caught by caller
    existing_table = client.get_table(base_table)

    try:
        client.get_table(target_table)
        print(f'{target_table} already exists')
    except NotFound:
        # Store old schema
        original_schema = existing_table.schema
        new_schema = []
        datatype_list = []

        # Instead of 'copy_table' operation perform the below:
        bq_table_split = base_table.split('.')
        bq_target_table_split = target_table.split('.')

        print(
            f'Performing operations on table: {bq_table_split[2].upper()} and creating table: {bq_target_table_split[2].upper()}')

        # root_bucket = 'dnd-data-preprod-cortex-dags'
        root_bucket = gcs_bucket.split('/')[0]
        results = get_dd03l_datatypes(base_table)
        table_data = get_table_data(root_bucket, base_table)
        sap_datatype_matrix = get_data_types(root_bucket)

        # Iterate through results (There should only be one as we are searching by table AND field)
        print('Finding original SAP datatypes and getting BQ datatype')
        for row in results[2]:
            output_list = [
                element for element in sap_datatype_matrix if element['SAP'] == row['datatype']]
            if len(output_list) > 0:
                bq_datatype = output_list[0]['BQ']
            current_object = {
                'field_name': row['fieldname'],
                'sap_datatype': row['datatype'],
                'bq_datatype': bq_datatype
            }
            datatype_list.append(current_object)

        # Iterate through original schema
        print('Updating Original Schema with correct BQ DataTypes')
        for row in original_schema:
            current_row_schema = row.to_api_repr()
            field_name = current_row_schema['name']
            field_type = current_row_schema['type']
            before_field_type = current_row_schema['type']
            field_mode = current_row_schema['mode']

            search_list = [element for element in datatype_list if element['field_name'].upper(
            ) == field_name.upper()]
            if len(search_list) > 0:
                field_type = search_list[0]['bq_datatype']
            if before_field_type != field_type:
                print(
                    f'Changed {before_field_type} to {field_type} for column {field_name}')
            new_schema.append(bigquery.SchemaField(
                field_name, field_type, field_mode))
        cdc_table = bigquery.Table(target_table, schema=new_schema)

        if table_data != []:
            if len(table_data['clustered_fields']) != 0:
                # Append clustering logic for fields
                cdc_table.clustering_fields = [
                    x.lower() for x in table_data['clustered_fields']]

            if len(table_data['partition_field']) != 0:
                # Append partitioning logic for required fields
                if table_data['partition_type'] == 'HOUR':
                    partition_type = bigquery.TimePartitioningType.HOUR
                elif table_data['partition_type'] == 'MONTH':
                    partition_type = bigquery.TimePartitioningType.MONTH
                elif table_data['partition_type'] == 'YEAR':
                    partition_type = bigquery.TimePartitioningType.YEAR
                else:
                    partition_type = bigquery.TimePartitioningType.DAY

                # Append partition logic for dates
                cdc_table.time_partitioning = bigquery.TimePartitioning(
                    type_=partition_type,
                    field=table_data['partition_field'].lower()
                )

        output_cdc_table = client.create_table(cdc_table)

        if not gen_test:
            sql = f"TRUNCATE TABLE `{target_table}`"
            job = client.query(sql)
            job.result()
        sql = f"ALTER TABLE `{target_table}` DROP COLUMN IF EXISTS is_deleted, DROP COLUMN IF EXISTS operation_flag"
        job = client.query(sql)
        job.result()
        print(
            f"Created target table {output_cdc_table.project}.{output_cdc_table.dataset_id}.{output_cdc_table.table_id}")


def check_create_hiertable(full_table, field):
    try:
        client.get_table(full_table)
    except NotFound:
        schema = [
            bigquery.SchemaField("mandt", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("parent", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("parent_org", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("child", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("child_org", "STRING", mode="NULLABLE"),
            bigquery.SchemaField(field, "STRING", mode="NULLABLE")
        ]

        table = bigquery.Table(full_table, schema=schema)
        table = client.create_table(table)
        print("Created {ft}".format(ft=full_table))


def create_view(target_table_name, sql):
    if sql != "":
        view_id = target_table_name
        view = bigquery.Table(view_id)
        view.view_query = sql
    else:
        view_id = target_table_name + "_view"
        view = bigquery.Table(view_id)
        view.view_query = f"SELECT * EXCEPT (recordstamp) FROM `{target_table_name}`"
    # Make an API request to create the view.
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")


def get_keys(dataset, source_table):
    query = "SELECT DISTINCT fieldname FROM `{dataset}.dd03l` WHERE KEYFLAG = 'X' AND fieldname != '.INCLUDE' AND tabname = \"{source_table}\"".format(
        dataset=dataset, source_table=source_table.upper())
    query_job = client.query(query)
    fields = []
    for row in query_job:
        column = row["fieldname"]
        fields.append(column)
    return fields


def copy_to_storage(gcs_bucket, prefix, directory, filename):
    try:
        bucket = storage_client.get_bucket(gcs_bucket)
    except:
        print(f"Error when accessing GCS bucket: {gcs_bucket}")
    blob = bucket.blob(f"{prefix}/{filename}")
    blob.upload_from_filename(f"{directory}/{filename}")
