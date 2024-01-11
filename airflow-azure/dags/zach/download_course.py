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
from utils.download_utils import lista_gen, find_last_true_occurrence
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance

with DAG(dag_id="download_course", 
         start_date=datetime(2024, 1, 10),
         catchup=False,
) as dag:
    

    @task(executor_config=define_k8s_specs('{{ dag_run.conf.get("claim_name") }}'))
    def set_jwt(**kwargs):
        ti: TaskInstance = kwargs["ti"] 
        dag_run: DagRun = ti.dag_run
        
        with open('/mnt/mydata/teste.txt', 'w') as f:
            f.write(dag_run.conf)
            print(dag_run.conf)
    
    @task(executor_config=define_k8s_specs('{{ dag_run.conf.get("claim_name") }}'))
    def get_jwt():
        with open('/mnt/mydata/teste.txt', 'r') as f:
            content = f.readlines()
        
        print(content)

    set_jwt() >> get_jwt()