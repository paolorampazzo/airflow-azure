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

claim_name = 'my-pvc'

with DAG(dag_id="prepare_download", 
         start_date=datetime(2024, 1, 10),
         catchup=False,
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

    @task
    def delete_pvc():
        from kubernetes import config, client
        
        config.load_incluster_config()
        v1 = client.CoreV1Api()

        # pvc = v1.read_namespaced_persistent_volume_claim(name=claim_name, 
        #                                                  namespace='airflow-azure-workers')
        
        
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

        return pages[:2]
    
    @task
    def get_parameters(**kwargs):
        import requests
        ti: TaskInstance = kwargs["ti"] 
        dag_run: DagRun = ti.dag_run
        link = kwargs['link']

        print(dag_run)
        print(dag_run.conf)
        version = dag_run.conf['version']
        cookies = dag_run.conf['cookies']
        name = link[link.rfind("/")+1:]
        # m3u8_page = f'https://dataengineer.io/api/v1/content/video/{version}/{name}/playlist.m3u8'
        # r = requests.get(m3u8_page, cookies={k['name']: k['value'] for k in cookies})
        # r = requests.get(m3u8_page, cookies={'jwt': cookies})

        # try:
        #     assert r.status_code == 200
        # except Exception as e:
        #     print('Erro', e)

        prefix = f'https://dataengineer.io/api/v1/content/video/{version}/'
        type = ('lecture' in name and 'lecture') or ('lab' in name and 'lab') or ('recording')
        lista_urls = [prefix + f'{name}/{type}{i}.ts' for i in range(0, 2000)]
        lista = [lista_gen(x) for x in lista_urls]       
        max_index = find_last_true_occurrence(lista) 
        
        return {'name': name, 'type': type, 'max_index': max_index, 
                'claim_name': claim_name}
    

    @task_group(group_id='group')
    def send_to_dag(parameters):
        
        @task
        def print_params(parameters):
            print(parameters)
    
        download_files = TriggerDagRunOperator(
            task_id="download_files_dag",
            trigger_dag_id="download_course",
            wait_for_completion=True,
            # deferrable=True,  # this parameters is available in Airflow 2.6+
            # poke_interval=5,
            # conf="{{ ti.xcom_pull(task_ids='upstream_task') }}"
            conf=parameters

        )
        
        print_params() >> download_files



    parameters_list = get_parameters.expand(link = get_links())

    # kubectl() >> send_to_dag.expand(parameters = parameters_list) >> delete_pvc()
    send_to_dag.expand(parameters = parameters_list) >> delete_pvc()
