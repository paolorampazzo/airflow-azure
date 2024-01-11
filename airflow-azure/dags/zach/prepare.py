"""Example DAG demonstrating the usage of dynamic task mapping."""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import task, task_group
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from utils.k8s_pvc_specs import define_k8s_specs 


with DAG(dag_id="prepare_download", 
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
        v1.delete_namespaced_persistent_volume_claim
        
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
    def set_jwt():
        with open('/mnt/mydata/teste.txt', 'w') as f:
            f.write('Oi')
    
    @task(executor_config=define_k8s_specs())
    def get_jwt():
        with open('/mnt/mydata/teste.txt', 'r') as f:
            content = f.readlines()
        
        print(content)

    @task
    def delete_pvc():
        from kubernetes import config, client
        import yaml
        
        config.load_incluster_config()
        v1 = client.CoreV1Api()
        v1.delete_namespaced_persistent_volume_claim(namespace="airflow-azure-workers", name="my-pvc")
    
    @task
    def get_links():
        import pickle
        with open('/opt/airflow/dags/repo/airflow-azure/dags/zach/pages.pkl', 'rb') as f:
            pages = pickle.load(f)

        return [1,2,3]
    
    @task
    def get_m3u8(link: int):
        print('retorno')
        return link
    
    @task
    def find_last_file(m3u8_file_str: int):
        print('retorno')
        return m3u8_file_str
    
    @task
    def download_file(file: int):
        print('retorno')
        return file
    
    m3u8 = get_m3u8.expand(link = get_links())
    last_file = find_last_file(m3u8)
    # download_file.expand(file = list(range(last_file)))

        
    kubectl() >> set_jwt() >> get_jwt() >> delete_pvc()
