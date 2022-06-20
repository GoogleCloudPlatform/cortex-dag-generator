# About
This generator reads the specified CDC table names, keys of the specified tables and merge table name and generates a merge query and a python script for each table for Cloud Composer or Apache Airflow. We recommend reading the instructions in the parent module, the [Cortex Data Foundation](https://github.com/GoogleCloudPlatform/cortex-data-foundation).

# Prerequisites:
The following steps must be completed before running this generator.
- [ ] An existing BigQuery Source Dataset that holds all source tables, each of which with ```recordstamp``` and ```operation_flag``` columns. Adjust the files in the `template_dag` and `template_sql` if the fields have different names.
- [ ] A GCS bucket created for holding the DAG python scripts and SQL scripts
- [ ] A GCS bucket created for logs that this generator writes to

# Cloudbuild Parameters:
The ```cloudbuild.cdc.yaml``` for this generator requires the following parameters
- ```_DS_RAW```: Source BigQuery Dataset where data is replicated
- ```_DS_CDC```: Target BigQuery Dataset for the results of CDC processing
- ```_PJID_SRC```: Source Google Cloud Project ID
- ```_PJID_TGT```: Target Google Cloud Project ID
- ```_GCS_BUCKET```: Name of the bucket created for transient holding the DAG scripts and SQL scripts
- ```_TEST_DATA```: If set to true, the test records in the generated test tables will be copied from the raw landing dataset into the CDC tables
- ```_SQL_FLAVOUR```: 'S4' or 'ECC'. Default: S4
- ```_GCS_LOG_BUCKET```: GCS bucket created for logs that this generator writes to
- ```_GEN_EXT```: Generate external DAGs. Requires configuration of external sources unless using _TEST_DATA=true (see README instructions in the [data foundation](https://github.com/GoogleCloudPlatform/cortex-data-foundation)).


# Run Options
- Clone the repository into your Cloud Shell Editor or an IDE of your choice 
- Ensure gcloud SDK is installed, if you choose your own IDE
- Make required changes in the ```settings.yaml``` to add / delete the required tables and run frequencies.  Save the file.
- Adjust the ```sets.yaml``` to add / delete the required SAP datasets to be flattened and run frequencies.  Save the file.

The generator can be run from the Cloud Console using the ```gcloud builds submit ...``` command or by configuring a Cloud Builds trigger that runs automatically upon push to a Cloud Source Repository branch

# Results
- The generated python scripts will be copied to ```gs://${_GCS_BUCKET}/dags```
- The generated SQL scripts will be copied to ```gs://${_GCS_BUCKET}/data/bq_data_replication```


