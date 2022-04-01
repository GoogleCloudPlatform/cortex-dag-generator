# About
This generator reads the specified CDC table names, keys of the specified tables and merge table name and generates a merge query and a python script for each table for Cloud Composer or Apache Airflow.

# Prerequisites:
The following steps must be completed before running this generator.
- [ ] An existing BigQuery Source Dataset that holds all source tables, each of which with ```recordtimestamp``` and ```operation``` columns
- [ ] A running Cloud Composer Instance in the target Google Cloud project 
- [ ] A tested Airflow BQ Connection to the source Google Cloud Project
- [ ] A GCS bucket created for holding the DAG python scripts and SQL scripts
- [ ] A GCS bucket created for logs that this generator writes to

# Cloudbuild Parameters:
The ```cloudbuild.yaml``` for this generator requires the following parameters
- ```_DS_SRC```: Source BigQuery Dataset
- ```_DS_TGT```: Target BigQuery Dataset
- ```_PJID_SRC```: Source Google Cloud Project ID
- ```_PJID_TGT```: Target Google Cloud Project ID
- ```_GCS_BUCKET```: Name of the bucket created for transient holding the DAG scripts and SQL scripts
- ```_GCS_LOG_BUCKET```: GCS bucket created for logs that this generator writes to


# Run Options
- Clone the repository into your Cloud Shell Editor or an IDE of your choice [Ensure gcloud SDK isinstalled, if you choose your own IDE]
- Make required changes in the ```settings.yaml``` to add / delete the required tables and run frequencies.  Save the file.

The generator can be run from the Cloud Console using the ```gcloud builds submit ...``` command or by configuring a Cloud Builds trigger that runs automatically upon push to a Cloud Source Repository branch

# Results
- The generated python scripts will be copied to ```gs://${_GCS_BUCKET}/dags```
- The generated SQL scripts will be copied to ```gs://${_GCS_BUCKET}/data/bq_data_replication```


