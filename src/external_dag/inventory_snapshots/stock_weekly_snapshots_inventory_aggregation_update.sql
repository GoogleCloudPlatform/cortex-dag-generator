# Copyright 2023 Google LLC
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

## CORTEX-CUSTOMER: These procedures need to execute for inventory views to work.
## please check the ERD linked in the README for dependencies. The procedures
## can be scheduled with Cloud Composer with the provided templates or ported
## into the scheduling tool of choice. These DAGs will be executed from a different
## directory structure in future releases.
## PREVIEW

CREATE OR REPLACE PROCEDURE
`{{ project_id_src }}.{{ dataset_cdc_processed }}.UpdateWeeklyInventoryAggregation`(
  week_start_date_cut_off DATE)
BEGIN
  DELETE FROM
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.weekly_inventory_aggregation`
  WHERE
    week_end_date >= week_start_date_cut_off;

  INSERT INTO
  `{{ project_id_src }}.{{ dataset_cdc_processed }}.weekly_inventory_aggregation`
  (mandt, werks, matnr, charg, lgort, bukrs, meins, waers, stock_characteristic,
    cal_year, cal_week, week_end_date,
    total_weekly_movement_amount, total_weekly_movement_quantity)
  SELECT
    src.mandt,
    src.werks,
    src.matnr,
    src.charg,
    src.lgort,
    src.bukrs,
    src.meins,
    src.waers,
    StockCharacteristicsConfig.stock_characteristic,
    EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', CAST(datedim.weekEndDate AS STRING))) AS cal_year,
    EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', CAST(datedim.weekEndDate AS STRING))) AS cal_week,
    PARSE_DATE('%Y%m%d', CAST(datedim.WeekEndDate AS STRING)) AS week_end_date,
    {% if sql_flavour == 'ecc' -%}
      SUM(IF(src.shkzg = 'H', (src.dmbtr * -1), src.dmbtr)) AS total_weekly_movement_amount,
      SUM(IF(src.shkzg = 'H', (src.menge * -1), src.menge)) AS total_weekly_movement_quantity
      {% else -%}
      SUM(src.dmbtr_stock) AS total_weekly_movement_amount,
      SUM(src.stock_qty) AS total_weekly_movement_quantity
      {% endif -%}
  FROM
  {% if sql_flavour == 'ecc' -%}
    `{{ project_id_src }}.{{ dataset_cdc_processed }}.mseg` AS src
    {% else -%}
    `{{ project_id_src }}.{{ dataset_cdc_processed }}.matdoc` AS src
    {% endif -%}
    LEFT JOIN
    `{{ project_id_src }}.{{ dataset_cdc_processed }}.calendar_date_dim` AS datedim
    ON
    {% if sql_flavour == 'ecc' -%}
        src.budat_mkpf = datedim.date
        {% else -%}
        src.budat = datedim.Date
        {% endif -%}
    LEFT JOIN
    `{{ project_id_src }}.{{ dataset_cdc_processed }}.stock_characteristics_config` AS StockCharacteristicsConfig
    ON
      src.mandt = StockCharacteristicsConfig.mandt
      AND src.sobkz = StockCharacteristicsConfig.sobkz
      {% if sql_flavour == 'ecc' -%}
      AND src.bwart = StockCharacteristicsConfig.bwart
        AND src.shkzg = StockCharacteristicsConfig.shkzg
        AND src.insmk = StockCharacteristicsConfig.insmk
      {% else -%}
      AND src.bstaus_sg = StockCharacteristicsConfig.bstaus_sg
      {% endif -%}
    WHERE
        {% if sql_flavour == 'ecc' -%}
      src.budat_mkpf >= week_start_date_cut_off
        {% else -%}
      src.budat >= week_start_date_cut_off
      {% endif -%}
      AND src.mandt = '{{ mandt }}'
      GROUP BY
        src.mandt,
        src.werks,
        src.matnr,
        src.charg,
        src.meins,
        src.waers,
        src.lgort,
        src.bukrs,
        StockCharacteristicsConfig.stock_characteristic,
        datedim.WeekEndDate;

END;
