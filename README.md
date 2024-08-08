# Cortex CDC DAG generator for SAP
This generator reads the specified CDC table names, keys of the specified tables and merge table
name and generates a merge query and a python script for each table for Cloud Composer or Apache
Airflow. We recommend reading the instructions in the parent module, the
[Cortex Data Foundation](https://github.com/GoogleCloudPlatform/cortex-data-foundation).

# Prerequisites:
The following steps must be completed before running this generator.
- [ ] An existing BigQuery Source Dataset that holds all source tables, each of
which with `recordstamp` and `operation_flag` columns. Adjust the files
in the `template_dag` and `template_sql` if the fields have different names.
- [ ] A GCS bucket created for holding the DAG python scripts and SQL scripts (target bucket).
- [ ] A GCS bucket created for logs that this generator writes to (logs bucket).
- [ ] `config/config.json` file with the values required for SAP deployment configuration
as described in the [Data Foundation README](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/README.md).
- [ ] Replicated DD03L table

> Configuration file for this repository is **`config/config.json`**.

# Cloudbuild Parameters:
The `cloudbuild.cdc.yaml` for this generator requires the following
parameters:
- `_GCS_BUCKET`: GCS bucket created for logs that this generator writes to (logs bucket).

# Run Options
- Clone the repository into your Cloud Shell Editor or an IDE of your choice.
- Ensure gcloud SDK is installed, if you choose your own IDE.
- Makes changes in `config/config.json` as described in the [Data Foundation README](https://github.com/GoogleCloudPlatform/cortex-data-foundation/blob/main/README.md) for SAP deployment
(that README refers to `config/config.json`).
- Make required changes in the `settings.yaml` to add / delete the required tables and run
 frequencies. Save the file.  - Adjust the `sets.yaml` to add / delete the required SAP datasets
 to be flattened and run frequencies. Save the file.

The generator can be run from the Cloud Console using the `gcloud builds submit ...` command or
by configuring a Cloud Builds trigger that runs automatically upon push to a Cloud Source
 Repository branch

# Results
- The generated python scripts will be copied to `gs://<targetBucket>/dags`
- The generated SQL scripts will be copied to `gs://<targetBucket>/data/bq_data_replication`

`targetBucket` - is a GCS bucket created for holding the DAG python scripts and SQL scripts
(`targetBucket` in `config/config.json`).

# Performance Optimization for Partitioned Table 
The following steps must be followed to generate the optimized CDC script for raw and cdc tables partitioned by recordstamp. 
Recommendation is to use this script for tables that are huge in size / that have low latency requirements.
The optimized script can be found at this path /src/template_sql/cdc_sql_partition_template.sql
Please follow the below steps to enable 
- make sure that the table for which you want to use this script is partitioned by recordstamp at a DAY level in RAW dataset
- add the option partition_flg: "Y"  
- add partition settings to enable DAY partition on recordstamp field. 
- Refer to the example given in the cdc_settings.yaml file for the table acdoca