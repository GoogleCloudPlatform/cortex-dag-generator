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

MERGE `${target_table}` T
USING (
  WITH 
    S1 AS (
      SELECT * FROM `${base_table}` s1
      WHERE recordstamp >= (
        SELECT IFNULL(MAX(recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
    ),
    T1 AS (
      SELECT ${keys}, MAX(recordstamp) AS recordstamp
      FROM `${base_table}`
      WHERE recordstamp >= (
        SELECT IFNULL(MAX(recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
      GROUP BY ${keys}
    )
  SELECT S1.*
  FROM S1
  INNER JOIN T1
    ON ${p_key_sub_query}
      AND S1.recordstamp = T1.recordstamp
  ) S
ON ${p_key}
WHEN NOT MATCHED THEN
  INSERT (${fields})
  VALUES (${fields})
WHEN MATCHED AND S.operation_flag = 'D' AND S.is_deleted = true THEN
  DELETE
WHEN MATCHED AND S.operation_flag = 'U' THEN
  UPDATE SET ${update_fields};
