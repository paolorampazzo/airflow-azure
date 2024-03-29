# # Licensed to the Apache Software Foundation (ASF) under one
# # or more contributor license agreements.  See the NOTICE file
# # distributed with this work for additional information
# # regarding copyright ownership.  The ASF licenses this file
# # to you under the Apache License, Version 2.0 (the
# # "License"); you may not use this file except in compliance
# # with the License.  You may obtain a copy of the License at
# #
# #   http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing,
# # software distributed under the License is distributed on an
# # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# # KIND, either express or implied.  See the License for the
# # specific language governing permissions and limitations
# # under the License.
# """
# Example DAG demonstrating the usage of the TaskFlow API to execute Python functions natively and within a
# virtual environment.
# """
# from __future__ import annotations

# import logging
# import shutil
# import sys
# import tempfile
# import time
# from pprint import pprint

# import pendulum

# from airflow import DAG
# from airflow.decorators import task
# from airflow.operators.python import ExternalPythonOperator, PythonVirtualenvOperator
# from airflow.operators.python import PythonOperator
# from airflow.models.param import Param
# from utils.k8s_specs import define_k8s_specs 
# import cx_Oracle

# log = logging.getLogger(__name__)

# PATH_TO_PYTHON_BINARY = sys.executable

# BASE_DIR = tempfile.gettempdir()


# with DAG(
#     dag_id="oracle_teste",
#     schedule=None,
#     start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
#     catchup=False,
#     tags=["example"],
#     # params={
#     #     "memory_request": Param(2, type="number", title="memory_request_gib (default = 2)"),
#     #     "memory_limit": Param(0, type="number", title="memory_limit_gib (default = infinity)"),
#     #     "cpu_request": Param(100, type="number", title="memory_request_gib (default = 100m)"),
#     # }
# ) as dag: 
    
#     # [START howto_operator_python]
#     # @task(task_id="print_the_context",
#     #       executor_config=define_k8s_specs(**get_specifications_from_ui_params(dag.params), other_specs={}))
#     @task(task_id="print_the_context")
#     def print_context(ds=None, **kwargs):
#         """Print the Airflow context and ds variable from the context."""
#         # pprint(kwargs)
#         # print(ds)
#         # print('TESTE')
#         # print(dag.params)
#         print('Teste')
#         cx_Oracle.init_oracle_client()
#         print('Funciona!')
#         try:
#            print(set(dag))
#            print('Deu certo1')
#         except:
#            print("Erro 1")
#         try:
#            print(dag.ti)
#            print('Deu certo2')
#         except:
#            print("Erro 2")

#         return "Whatever you return gets printed in the logs"
    
#     # def teste(**kwargs):
#     #    print('TI')
#     #    print(kwargs['ti'].dag_run.conf)
    
#     # start_task = PythonOperator(
#     #         task_id="start_task",
#     #         python_callable=teste,
#     #     )


#     run_this = print_context()
#     # start_task
#     # [END howto_operator_python]



    