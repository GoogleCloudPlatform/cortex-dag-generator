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

  WITH S1 AS (
      SELECT * FROM `${base_table}` order by recordstamp
  ),
    T1 AS (
    SELECT ${keys}, max(recordstamp) as recordstamp from `${base_table}` where operation_flag in ('U', 'I', 'L') group by ${keys} order by recordstamp
    ),
    D1 AS (
        SELECT ${keys_with_dt1_prefix}, DT1.recordstamp from `${base_table}` DT1,T1 where DT1.operation_flag ='D' and ${keys_comparator_with_dt1_t1} and DT1.recordstamp > T1.recordstamp order by recordstamp
    ),
    T1S1 AS (
    SELECT  S1.* EXCEPT(operation_flag,is_deleted) from S1 INNER JOIN T1 ON ${keys_comparator_with_t1_s1} and S1.recordstamp = T1.recordstamp
    )
    SELECT T1S1.* EXCEPT(recordstamp) FROM T1S1 
        LEFT OUTER JOIN D1 ON ${keys_comparator_with_t1s1_d1} and D1.recordstamp > T1S1.recordstamp
    WHERE D1.recordstamp  is null
