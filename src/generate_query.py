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
from string import Template
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.cloud import storage
from pathlib import Path

client = bigquery.Client()
storage_client = storage.Client()


def table_columns(bq_table, source_project):
    bq_table_split = bq_table.split(".")
    query = "SELECT  column_name FROM  {src_project}.{dataset}.INFORMATION_SCHEMA.COLUMNS WHERE table_name=\"{table_name}\"".format(
        dataset=bq_table_split[1],
        table_name=bq_table_split[2],
        src_project=source_project)
    query_job = client.query(query)  # Make an API request.
    seperator = ","
    update_fields = []
    columns_to_exclude = ['_PARTITIONTIME', 'operation_flag', 'is_deleted']
    fields = []
    for row in query_job:
        column = row["column_name"]
        if column not in columns_to_exclude:
            fields.append(column)
            update_fields.append(f"T.{column}=S.{column}")
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


def generate_runtime_sql(cdc_base_table, cdc_target_view, keys,
                         source_project):
    fields, update_fields = table_columns(cdc_base_table, source_project)
    if not fields:
        print(f"Schema could not be retrieved for {cdc_base_table}")
    keys_with_dt1_prefix = ",".join(add_prefix_to_keys('DT1', keys))
    keys_comparator_with_dt1_t1 = " and ".join(
        get_key_comparator(['DT1', 'T1'], keys))
    keys_comparator_with_t1_s1 = " and ".join(
        get_key_comparator(['T1', 'S1'], keys))
    keys_comparator_with_t1s1_d1 = " and ".join(
        get_key_comparator(['D1', 'T1S1'], keys))
    sql_template_file = open(
        os.path.dirname(os.path.abspath(__file__)) +
        "/template_sql/runtime_query_view.sql", "r")
    sql_template = Template(sql_template_file.read())
    generated_sql_template = sql_template.substitute(
        base_table=cdc_base_table,
        keys=",".join(keys),
        keys_with_dt1_prefix=keys_with_dt1_prefix,
        keys_comparator_with_t1_s1=keys_comparator_with_t1_s1,
        keys_comparator_with_dt1_t1=keys_comparator_with_dt1_t1,
        keys_comparator_with_t1s1_d1=keys_comparator_with_t1s1_d1)
    create_view(cdc_target_view, generated_sql_template)


def generate_sql(cdc_base_table, cdc_target_table, keys, source_project, gen_test):
    fields, update_fields = table_columns(cdc_base_table,source_project)
    if not fields:
        print(f"Schema could not be retrieved for {cdc_base_table}")
    p_key_list = get_key_comparator(['S', 'T'], keys)
    p_key_list_for_sub_query = get_key_comparator(['S1', 'T1'], keys)
    p_key = " and ".join(p_key_list)
    p_key_sub_query = " and ".join(p_key_list_for_sub_query)
    try:
        sql_template_file = open(
            os.path.dirname(os.path.abspath(__file__)) +
            "/template_sql/cdc_sql_template.sql", "r")
        sql_template = Template(sql_template_file.read())

        generated_sql_template = sql_template.substitute(
            base_table=cdc_base_table,
            target_table=cdc_target_table,
            p_key=p_key,
            fields=fields,
            update_fields=update_fields,
            keys=",".join(keys),
            p_key_sub_query=p_key_sub_query)
        cdc_sql_filename = "cdc_" + cdc_base_table.replace(".", "_") + ".sql"
        cdc_sql_filepath = "generated_sql/" + cdc_sql_filename
        generated_sql_file = open(
            Path.joinpath(Path(__file__).resolve().parents[1], cdc_sql_filepath),
            "w")
        generated_sql_file.write(generated_sql_template)
        generated_sql_file.close()
    except:
        print(f"TECHNICAL Error - Unable to open SQL template or generate SQL")
        raise NotFound("SQL file")

    try:
        check_create_target(cdc_base_table, cdc_target_table, gen_test)
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
        cdc_dag_filename = "cdc_" + cdc_base_table.replace(".", "_") + "_hier.py"
        cdc_dag_filepath = "../generated_dag/" + cdc_dag_filename
        generated_dag_file = open(cdc_dag_filepath, "w+")
        generated_dag_file.write(generated_dag_template)
        generated_dag_file.close()
    except:
         print(f"TECHNICAL Error accessing template or generating DAG file")
         raise NotFound("DAG file")


def get_key_comparator(table_prefix, keys):
    p_key_list = []
    for key in keys:
        p_key_list.append("{0}.{2}={1}.{2}".format(table_prefix[0],
                                                   table_prefix[1], key))
    return p_key_list


def get_comparator_with_select(table_name, keys):
    p_key_list = []
    for key in keys:
        p_key_list.append(f"{key} not in (SELECT {key} from {table_name})")
    return p_key_list


def add_prefix_to_keys(prefix, keys):
    prefix_keys = []
    for key in keys:
        prefix_keys.append("{0}.{1}".format(prefix, key))
    return prefix_keys


def check_create_target(base_table, target_table, gen_test):
    #If the base table hasn't been replicated, the exception will be caught by caller
    client.get_table(base_table)
    try:
        client.get_table(target_table)
    except NotFound:
        job = client.copy_table(base_table, target_table)
        job.result()
        if not gen_test:
            sql = f"TRUNCATE TABLE {target_table}"
            job = client.query(sql)
            job.result()
        sql = f"ALTER TABLE {target_table} DROP COLUMN IF EXISTS is_deleted, DROP COLUMN IF EXISTS operation_flag"
        job = client.query(sql)
        job.result()
        print(f"Created {target_table}")


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
        view.view_query = f"SELECT * except(recordstamp) FROM `{target_table_name}`"
    # Make an API request to create the view.
    view = client.create_table(view, exists_ok=True)
    print(f"Created {view.table_type}: {str(view.reference)}")


def get_keys(dataset, source_table):
    query = "SELECT  fieldname FROM {dataset}.dd03l where KEYFLAG = 'X' AND fieldname != '.INCLUDE' and tabname = \"{source_table}\"".format(
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


# def get_nodes(src_dataset, mandt, setname, setclass, org_unit,
#               table, select_fields, full_table):
#     sets_tables = []

#     query = """SELECT  setname, setclass, subclass, lineid, subsetcls, subsetscls, subsetname
#              FROM  {src_dataset}.setnode
#              WHERE setname = \'{setname}\' and setclass = \'{setclass}\'
#              and subclass = \'{org_unit}\' and mandt = \'{mandt}\' """.format(
#         src_dataset=src_dataset,
#         setname=setname,
#         mandt=mandt,
#         setclass=setclass,
#         org_unit=org_unit)

#     query_job = client.query(query)

#     for set in query_job:
#         nodes = get_leafs_children( src_dataset, mandt, set,
#                                    table, select_fields, full_table)
#         print(nodes)
#         sets_tables.append(nodes)
#         node_d = dict() = nodes


#     return sets_tables


# def get_leafs_children( src_dataset, mandt, row, table, field, full_table):
#     node_dict = dict()
#     # TODO: would be nice to implement multithreaded calls

#     node_dict = {
#         "mandt": mandt,
#         "parent": row['setname'],
#         "parent_org": row['subclass'],
#         "child": row['subsetname'],
#         "child_org": row['subsetscls']
#     }

#     #Get values from setleaf (only lower child sets have these)
#     query = """SELECT  valsign, valoption, valfrom, valto
#             FROM  {src_dataset}.setleaf
#             WHERE setname = \'{setname}\'
#             and setclass = \'{setclass}\'
#             and subclass = \'{subclass}\'
#             and mandt = \'{mandt}\' """.format(src_dataset=src_dataset,
#                                                setname=row['subsetname'],
#                                                mandt=mandt,
#                                                setclass=row['subsetcls'],
#                                                subclass=row['subsetscls'])

#     leafs = client.query(query)

#     #Get values from actual MD tables (e.g., Costs center, GL Accounts, etc)
#     for setl in leafs:

#         #Build WHERE clause from SETLEAF info and clause from sets.yaml
#         if setl['valoption'] == 'EQ':
#             where_cls = " {field}  = \'{valfrom}\' ".format(
#                 field=field, valfrom=setl['valfrom'])
#         elif setl['valoption'] == 'BT':
#             where_cls = " {field} between \'{valfrom}\' and  \'{valto}\' ".format(
#                 field=field, valfrom=setl['valfrom'], valto=setl['valto'])

#         query = """ SELECT {field}
#                     from {src_dataset}.{table}
#                     where mandt  = \'{mandt}\'
#                     and {where_clause}""".format(field=field,
#                                                  src_dataset=src_dataset,
#                                                  table=table,
#                                                  mandt=mandt,
#                                                  where_clause=where_cls)
#         #print(query)
#         ranges = client.query(query)
#         for line in ranges:
#             node_dict = {
#                 "mandt": mandt,
#                 "parent": row['setname'],
#                 "parent_org": row['subclass'],
#                 "child": row['subsetname'],
#                 "child_org": row['subsetscls'],
#                 field: line[field]
#             }
#         #Recursive call for child dataset

#     get_nodes(src_dataset, mandt, row['subsetname'],
#               row['subsetcls'], row['subsetscls'], table, field, full_table)
#     return node_dict  #This may only have a parent/child
