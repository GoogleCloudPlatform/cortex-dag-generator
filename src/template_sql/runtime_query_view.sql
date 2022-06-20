--  Copyright 2022 Google LLC
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--      https://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.

WITH 
  T1 AS (
    SELECT ${keys}, MAX(recordstamp) AS recordstamp 
    FROM `${base_table}` 
    WHERE operation_flag IN ('U', 'I', 'L') 
    GROUP BY ${keys} 
  ),
  D1 AS (
    SELECT ${keys_with_dt1_prefix}, DT1.recordstamp 
    FROM `${base_table}` AS DT1
    CROSS JOIN T1 
    WHERE DT1.operation_flag ='D' 
      AND ${keys_comparator_with_dt1_t1} 
      AND DT1.recordstamp > T1.recordstamp 
  ),
  T1S1 AS (
    SELECT S1.* EXCEPT (operation_flag, is_deleted) 
    FROM `${base_table}` AS S1 
    INNER JOIN T1 
    ON ${keys_comparator_with_t1_s1} 
      AND S1.recordstamp = T1.recordstamp
  )
SELECT T1S1.* EXCEPT (recordstamp) 
FROM T1S1 
LEFT OUTER JOIN D1 
  ON ${keys_comparator_with_t1s1_d1} 
    AND D1.recordstamp > T1S1.recordstamp
WHERE D1.recordstamp IS NULL
