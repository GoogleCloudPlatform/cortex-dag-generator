--  Copyright 2021 Google Inc.

--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at

--      http://www.apache.org/licenses/LICENSE-2.0

--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.

DECLARE max_rstamp TIMESTAMP;

DECLARE max_raw_stamp TIMESTAMP;

DECLARE processed_date ARRAY<DATE>;

set max_raw_stamp = (SELECT TIMESTAMP_SUB((SELECT MAX(recordstamp) FROM `${base_table}`), INTERVAL 5 SECOND)) ;

SET max_rstamp = (SELECT IFNULL(MAX(recordstamp), TIMESTAMP('1940-12-25 05:30:00+00')) FROM `${target_table}`);


SET processed_date = (
WITH
      S01 AS (
        SELECT * FROM `${base_table}`
        WHERE recordstamp >= max_rstamp and recordstamp <= max_raw_stamp
      ),

      -- To handle occasional dups from SLT connector

      S11 AS (
        SELECT * EXCEPT(row_num)
        FROM (
           SELECT *, ROW_NUMBER() OVER (PARTITION BY MANDT, RSART, RSNUM, RSPOS ORDER BY recordstamp desc) AS row_num
          FROM S01
        )
        WHERE row_num = 1
)

select ARRAY_AGG(distinct date(T.recordstamp)) from `${target_table}` T inner join S11 S on  S.`MANDT` = T.`MANDT` AND S.`RSART` = T.`RSART` AND S.`RSNUM` = T.`RSNUM` AND S.`RSPOS` = T.`RSPOS`
);


MERGE `${target_table}` AS T
USING (
  WITH
    S0 AS (
      SELECT * FROM `${base_table}`
      WHERE recordstamp >= max_rstamp and recordstamp <= max_raw_stamp
    ),
    -- To handle occasional dups from SLT connector
    S1 AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ${keys}, recordstamp ORDER BY recordstamp) AS row_num
        FROM S0
      )
      WHERE row_num = 1
    ) SELECT distinct S1.* FROM S1 ) AS S
ON
date(T.`recordstamp`) IN UNNEST(processed_date) AND
${p_key}

-- ## CORTEX-CUSTOMER You can use "`is_deleted` = true" condition along with "operation_flag = 'D'",
-- if that is applicable to your CDC set up.

WHEN NOT MATCHED AND IFNULL(S.operation_flag, 'I') != 'D' THEN
  INSERT (${fields})
  VALUES (${fields})
WHEN MATCHED AND S.operation_flag = 'D' THEN
  DELETE
WHEN MATCHED AND S.operation_flag IN ('I','U') THEN
  UPDATE SET ${update_fields};