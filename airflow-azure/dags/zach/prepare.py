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



with DAG(dag_id="prepare_download", 
         start_date=datetime(2024, 1, 10),
         catchup=False,
         max_active_runs = 10,
         max_active_tasks = 100,
         params={
         "version": Param('v3', enum=["v1", "v2", "v3"]),
         "cookies": Param('', type='string')
     },
) as dag:
    
    @task
    def kubectl():
        from kubernetes import config, client
        import yaml
        
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        
        yaml_content = lambda claim_name = claim_name: f"""
        apiVersion: v1
        kind: PersistentVolumeClaim
        metadata:
            name: {claim_name}
        spec:
            accessModes:
                - ReadWriteOnce  # or ReadWriteMany, ReadOnlyMany based on your requirements
            resources:
                requests:
                    storage: 1Gi  # Specify the amount of storage you need
        """

        resource = yaml.safe_load(yaml_content())
        api_response = v1.create_namespaced_persistent_volume_claim('airflow-azure-workers', 
                                                                    resource)


    @task(executor_config=define_k8s_specs(claim_name))
    def set_jwt():
        with open('/mnt/mydata/teste.txt', 'w') as f:
            f.write('Oi')
    
    @task(executor_config=define_k8s_specs(claim_name))
    def get_jwt():
        with open('/mnt/mydata/teste.txt', 'r') as f:
            content = f.readlines()
        
        print(content)

    # @task(trigger_rule=TriggerRule.ONE_DONE)
    @task()
    def delete_pvc():

        from kubernetes import config, client
        
        config.load_incluster_config()
        v1 = client.CoreV1Api()

        patch_payload = [
                {"op": "remove", "path": "/metadata/finalizers"}
            ]


        v1.delete_namespaced_persistent_volume_claim(namespace="airflow-azure-workers", name=claim_name)
        v1.patch_namespaced_persistent_volume_claim(name=claim_name, namespace="airflow-azure-workers", 
                                                    body=patch_payload)
    @task
    def get_links():
        import pickle
        with open('/opt/airflow/dags/repo/airflow-azure/dags/zach/pages.pkl', 'rb') as f:
            pages = pickle.load(f)

        return pages[:10]
    
    @task
    def get_parameters(**kwargs):
        import requests
        import json

        ti: TaskInstance = kwargs["ti"] 
        dag_run: DagRun = ti.dag_run
        link = kwargs['link']

        print(dag_run)
        print(dag_run.conf)
        version = dag_run.conf.get('version', 'v3')
        cookies = dag_run.conf.get('cookies', '')
        name = link[link.rfind("/")+1:]

        prefix = f'https://dataengineer.io/api/v1/content/video/{version}/'
        type = ('lecture' in name and 'lecture') or ('lab' in name and 'lab') or ('recording')
        lista_urls = [prefix + f'{name}/{type}{i}.ts' for i in range(0, 2000)]
        lista = [lista_gen(x) for x in lista_urls]       
        max_index = find_last_true_occurrence(lista) 
        
        # max_index = min(5, max_index)
        return {'name': name, 'type': type, 'max_index': max_index,
                'version': version}
    
    download_files = TriggerDagRunOperator.partial(
        task_id="download_files_dag",
        trigger_dag_id="download_course",
        wait_for_completion=True,
        weight_rule='upstream'
    )


    
    parameters_list = get_parameters.expand(link = get_links())

    download_obj = download_files.expand(conf = parameters_list)
    kubectl() >> download_obj >> delete_pvc()
