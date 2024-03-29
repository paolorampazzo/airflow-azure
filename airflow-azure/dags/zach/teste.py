"""Example DAG demonstrating the usage of dynamic task mapping."""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import task, task_group
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from kubernetes.client import models as k8s
from utils.k8s_pvc_specs import define_k8s_specs 
from utils.download_utils import lista_gen, find_last_true_occurrence, claim_name
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.trigger_rule import TriggerRule
from utils.download_utils import claim_name
from airflow.models import Variable


with DAG(dag_id="teste", 
         start_date=datetime(2024, 1, 10),
         catchup=False,
         params={
         "version": Param('v3', enum=["v1", "v2", "v3"]),
         "cookies": Param('', type='string')
     },
) as dag:
    
    @task(executor_config=define_k8s_specs(claim_name = claim_name, node_selector=[{'key': 'kubernetes.azure.com/agentpool',
                                                          'operator': 'In', 'values': ['basic10']},
                                                          {'key': 'meusystem',
                                                          'operator': 'In', 'values': ['true']}]))
    def teste1():
        from utils.google_api import list_folder

        print(list_folder())

        
    
    # @task(executor_config=define_k8s_specs(node_selector=[{'key': 'kubernetes.azure.com/agentpool',
    #                                                       'operator': 'In', 'values': ['basic10']},
    #                                                       {'key': 'meussytem',
    #                                                       'operator': 'NotIn', 'values': ['true']}]))
    # def teste2():
    #     print(1)

    # teste1() >> teste2()
    teste1()