"""Example DAG demonstrating the usage of dynamic task mapping."""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from utils.k8s_pvc_specs import define_k8s_specs 



with DAG(dag_id="download_videos", 
         start_date=datetime(2024, 1, 10),
         catchup=False,
         params={
         "x": Param('', type="string"),
     },
) as dag:
    
    @task
    def kubectl():
        from kubernetes import config, client
        import yaml
        
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        
        yaml_content = """
        apiVersion: v1
        kind: PersistentVolumeClaim
        metadata:
            name: my-pvc
        spec:
            accessModes:
                - ReadWriteOnce  # or ReadWriteMany, ReadOnlyMany based on your requirements
            resources:
                requests:
                    storage: 1Gi  # Specify the amount of storage you need
        """

        resource = yaml.safe_load(yaml_content)
        api_response = v1.create_namespaced_persistent_volume_claim('airflow-azure-workers', 
                                                                    resource)


    @task(executor_config=define_k8s_specs())
    def get_jwt():
        return 'asds'
    
    @task
    def get_links():
        import pickle
        with open('pages.pkl', 'rb') as f:
            pages = pickle.load(f)

    @task
    def add_one(x: int):
        return x + 1                    

    @task
    def sum_it(values):
        total = sum(values)
        print(f"Total was {total}")

    added_values = add_one.expand(x=[1, 2, 3])
    sum_it(added_values)

    kubectl() >> get_jwt()
