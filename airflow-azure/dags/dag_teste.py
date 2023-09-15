# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example DAG demonstrating the usage of the TaskFlow API to execute Python functions natively and within a
virtual environment.
"""
from __future__ import annotations

import logging
import shutil
import sys
import tempfile
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
from airflow.models.param import Param
from utils.k8s_specs import define_k8s_specs 

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

BASE_DIR = tempfile.gettempdir()


def x():
    pass 


with DAG(
    dag_id="exemplo_do_git",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    params={
        "memory_request_gib": Param(2, type="number", title="memory_request_gib (default = 2)"),
        "memory_limit_gib": Param(0, type="number", title="memory_limit_gib (default = infinity)"),
        "cpu_request_milicore": Param(100, type="number", title="memory_request_gib (default = 100m)"),
        "cpu_limit_milicore": Param(0, type="number", title="memory_request_gib (default = infinity)"),
    }
) as dag:
    
    # memory_request = 

    # [START howto_operator_python]
    @task(task_id="print_the_context",
          executor_config=define_k8s_specs(memory_request='2Gi', other_specs={}))
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        print('TESTE')
        print(dag.params)
        return "Whatever you return gets printed in the logs"

    run_this = print_context()
    # [END howto_operator_python]



    