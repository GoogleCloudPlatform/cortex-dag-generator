# Dynamic Table Creation Assets

# What is partitioning_clusters.json

- Partitioning clusters is a JSON file that provides a mapping for each table.
- These Mappings contain the following 'key' requests
    - table_name
        - Table name is required to be a string (lowercase or uppercase) to define the SAP table in question
    - partition_field
        - Partition field is required to be a string, This field is be used as the partitioning field for the table created by Cortex
    - partition_type
        - Partitioning type is required to be a string, This field is used to define the partitioning behaviour of the table. In the following format:
            - HOUR
            - DAY
            - MONTH
            - YEAR
        - These formats are pulled from here:
            - https://cloud.google.com/bigquery/docs/partitioned-tables#choose_daily_hourly_monthly_or_yearly_partitioning
        - and here:
            - https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.table.TimePartitioningType.html#google.cloud.bigquery.table.TimePartitioningType
    - clustered_fields
        - Clustered fields is required to be a list of strings, This field is used to define the columns of a table to cluster on. It can be a single string or many.

## How to use:

1. Update the partioning_clusters.json with your required settings for your data
2. Create a folder called referenced_data in your Target DAGS bucket for CDC
3. Upload the JSON file with the name: 'partitioning_clusters.json'


# What is sap_datatypes_to_bq.json
- This JSON file is used as a reference to override SAP types from DD03L (Metadata table from SAP) to the equivalent typings in BigQuery
- This mapping has mostly been created utilising the following reference:
    - https://cloud.google.com/solutions/sap/docs/bq-connector/latest/planning#default_data_type_mapping
- Anytime DATS is referenced in this MAP and converted to date, same with TIMS, these are required for Cortex views to work as intended if your source data doesn't match the Date or Time Schema in BQ

## How to use:

1. Update the sap_datatypes_to_bq.json with your required settings for your data
2. Create a folder called referenced_data in your Target DAGS bucket for CDC
3. Upload the JSON file with the name: 'sap_datatypes_to_bq.json'