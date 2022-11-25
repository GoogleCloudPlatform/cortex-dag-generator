CREATE OR REPLACE TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.currency_decimal`AS (
  SELECT DISTINCT
    tcurx.CURRKEY,
    CAST(POWER(10, 2 - COALESCE(tcurx.CURRDEC, 0)) AS NUMERIC) AS CURRFIX
  FROM
    `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurx` AS tcurx );
