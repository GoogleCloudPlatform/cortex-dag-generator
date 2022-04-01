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
from pathlib import Path

basedir = Path(__file__).parent.parent
scriptsdir = '{0}/scripts'.format(Path(__file__).parent)
sys.path.append('{0}/src'.format(basedir))
# from main import execute_transformation_query, main


class Helper():

    def set_env_var(key, value):
        os.environ[key] = value

    def loadData(dataset, dir):
        myCmd = 'sh {0}/load_test_data_bq_table.sh '.format(
            scriptsdir) + dataset + " " + dir
        os.system(myCmd)

    def create_directory(dir):
        myCmd = "mkdir " + dir
        os.system(myCmd)

    def delete_table(dataset, table):
        myCmd = 'sh {0}/delete_table.sh '.format(
            scriptsdir) + dataset + " " + table
        os.system(myCmd)

    def delete_file(dir):
        myCmd = 'sh {0}/delete_file.sh '.format(scriptsdir) + dir
        os.system(myCmd)

    def create_dataset(dataset):
        myCmd = 'sh {0}/create_dataset.sh '.format(scriptsdir) + dataset
        os.system(myCmd)

    def clean(source_ds, target_ds, dir, table_prefix):
        myCmd = 'sh {0}/clear.sh '.format(
            scriptsdir
        ) + source_ds + " " + target_ds + " " + dir + " " + table_prefix
        os.system(myCmd)

    def run_query_from_file(sql_file_path):
        myCmd = 'sh {0}/query.sh '.format(scriptsdir) + sql_file_path
        os.system(myCmd)

    def dump_result(project, dataset, table, order_by, local_output, filename):
        myCmd = 'sh {0}/extract.sh '.format(
            scriptsdir
        ) + project + " " + dataset + " " + table + " " + order_by + " " + local_output + " " + filename
        os.system(myCmd)
