# Copyright 2021 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import glob
import pytest
from pathlib import Path
import filecmp
from helper import Helper

basedir = Path(__file__).parent.parent
testdir = Path(__file__).parent
sys.path.append('{0}/src'.format(basedir))
from generate_query import *
from google.cloud import bigquery

data = {
    'project_id': os.environ['TEST_PROJECT_ID'],
    'source_dataset_id': os.environ['SOURCE_DATASET'],
    'target_dataset_id': os.environ['TARGET_DATASET'],
}

test_directory = []
for path in glob.glob('{0}/test*/'.format(testdir)):
    test_directory.append(path)

Helper.create_dataset(data['source_dataset_id'])
Helper.create_dataset(data['target_dataset_id'])

client = bigquery.Client(project=data['project_id'])


@pytest.mark.functional
@pytest.mark.parametrize("dir", test_directory)
def test_eval(dir):
    seperator = "*" * 60
    print("\n" + seperator)
    print("Testing test case " + dir)
    directory = dir.replace(".", "").replace("/", "")
    Helper.clean(data['project_id'] + ":" + data['source_dataset_id'],
                 data['project_id'] + ":" + data['target_dataset_id'], dir,
                 directory)
    Helper.create_directory("{0}/generated_sql".format(testdir))
    Helper.loadData(data['source_dataset_id'], directory)
    keys_from_file = open(dir + "keys.txt").read()
    keys = keys_from_file.split(",")
    generate_sql(
        data['project_id'] + "." + data['source_dataset_id'] + "." + directory +
        "_input",
        data['project_id'] + "." + data['target_dataset_id'] + "." + directory,
        keys, data['project_id'])
    cdc_base_table = data['project_id'] + "." + data[
        'source_dataset_id'] + "." + directory + "_input"
    cdc_sql_filename = "cdc_" + cdc_base_table.replace(".", "_") + ".sql"
    sql_file_path = "{0}/generated_sql/".format(testdir) + cdc_sql_filename

    Helper.run_query_from_file(sql_file_path)
    Helper.dump_result(data['project_id'], data['target_dataset_id'], directory,
                       keys_from_file, directory, "output")
    retVal_table = filecmp.cmp(dir + "output.js",
                               dir + "expected_output.json",
                               shallow=False)
    generate_runtime_sql(
        data['project_id'] + "." + data['source_dataset_id'] + "." + directory +
        "_input", data['project_id'] + "." + data['target_dataset_id'] + "." +
        directory + "_runtime", keys, data['project_id'])
    Helper.dump_result(data['project_id'], data['target_dataset_id'],
                       directory + "_runtime_view", keys_from_file, directory,
                       "outputview")

    retVal_view = filecmp.cmp(dir + "outputview.js",
                              dir + "expected_view.json",
                              shallow=False)

    if (retVal_view and retVal_table):
        Helper.clean(data['project_id'] + ":" + data['source_dataset_id'],
                     data['project_id'] + ":" + data['target_dataset_id'], dir,
                     directory)
    assert retVal_view == True and retVal_table == True
