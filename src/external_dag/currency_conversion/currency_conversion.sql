CREATE OR REPLACE TABLE `{{ project_id_src }}.{{ dataset_cdc_processed }}.currency_conversion` AS
(
  SELECT
  curr.mandt,
  curr.kurst,
  curr.fcurr,
  curr.tcurr,
IF
  (curr.ukurs < 0, 1 / ABS(curr.ukurs), curr.ukurs) AS ukurs,
  PARSE_DATE('%Y%m%d', CAST(99999999 - CAST(gdatu AS INT) AS STRING)) AS start_date,
IF
  (LEAD(PARSE_DATE('%Y%m%d',
        CAST(99999999 - CAST(curr.gdatu AS INT) AS STRING))) OVER (PARTITION BY curr.mandt, curr.kurst, curr.fcurr, curr.tcurr ORDER BY curr.gdatu DESC) IS NULL,
    DATE_ADD(PARSE_DATE('%Y%m%d',
        CAST(99999999 - CAST(curr.gdatu AS INT) AS STRING)), INTERVAL 1000 YEAR),
    DATE_SUB(LEAD(PARSE_DATE('%Y%m%d',
          CAST(99999999 - CAST(curr.gdatu AS INT) AS STRING))) OVER (PARTITION BY curr.mandt, curr.kurst, curr.fcurr, curr.tcurr ORDER BY curr.gdatu DESC), INTERVAL 1 DAY)) AS end_date
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurr` AS curr
  -- CORTEX-CUSTOMER: Change dates to a shorter range if applicable
  --- TODO:the following piece of the code can be re-structured to handle this in a better way.
  -- TODO: Feed frequency/date from dynamically
UNION ALL
SELECT
  DISTINCT curr.mandt,
  curr.kurst,
  curr.fcurr,
  curr.fcurr AS tcurr,
  1 AS ukurs,
  CAST('1990-01-01' AS date) AS start_date,
  CAST('3000-01-01' AS date) AS end_date
FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.tcurr` AS curr
)