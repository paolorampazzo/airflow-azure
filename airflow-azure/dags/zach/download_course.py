"""Example DAG demonstrating the usage of dynamic task mapping."""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import task, task_group
from airflow.models.dag import DAG
from airflow.models.param import Param
from kubernetes.client import models as k8s
from utils.k8s_pvc_specs import define_k8s_specs
from utils.download_utils import claim_name, lista_gen
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
import json


with DAG(dag_id="download_course", 
         start_date=datetime(2024, 1, 10),
         catchup=False,
) as dag:
    
    @task()
    def get_metadata(**kwargs):
        ti: TaskInstance = kwargs["ti"] 
        dag_run: DagRun = ti.dag_run

        print(dag_run.conf)
        
        metadata = dag_run.conf

        # return [{'name': metadata['name'], 
        #          'type': metadata['type'], 
        #         'i': x,
        #         'version': metadata['version']} for x in range(metadata['max_index']+1)]
        return [{'name': metadata['name'], 
                 'type': metadata['type'], 
                'i': x,
                'version': metadata['version']} for x in range(100)]
    
    @task(executor_config=define_k8s_specs(claim_name = claim_name,
                                           node_selector=[{'key': 'kubernetes.azure.com/agentpool',
                                                          'operator': 'In', 'values': ['basic10']},
                                                          {'key': 'meusystem',
                                                          'operator': 'NotIn', 'values': ['true']}]))
    def download_file(metadata):
        from requests import get
        from requests.exceptions import ConnectTimeout
        from os import makedirs
        
        name = metadata['name']
        
        type = metadata['type']
        i = metadata['i'] 
        version = metadata['version']
 
 
        folder_path = f'/mnt/mydata/{name}'
        
        prefix = f'https://dataengineer.io/api/v1/content/video/{version}/'
        type = ('lecture' in name and 'lecture') or ('lab' in name and 'lab') or ('recording')
        # name = raw_url[raw_url.rfind('/v3/')+4:raw_url.rfind('/')]

        # lista_urls = [prefix + f'{name}/{type}{i}.ts' for i in range(0, 2000)]
        # lista = [lista_gen(x) for x in lista_urls]

        try:
            makedirs(folder_path)
        except:
            pass


        url = prefix + f'{name}/{type}{i}.ts'

        file_name = f"{type}{i}.ts"
        file_path = f"{folder_path}/{file_name}"

        print('Downloading', file_name, 'to', file_path)

        try:
            response = get(url, timeout=3)
        except Exception as e:
            if type(e) == ConnectTimeout:
                raise Exception('timeout em' + str(url))

            # Save the file
            with open(file_path, "wb") as file:
                file.write(response.content)

    @task(executor_config=define_k8s_specs(claim_name = claim_name))
    def get_jwt():
        with open('/mnt/mydata/teste.txt', 'r') as f:
            content = f.readlines()
        
        print(content)

    metadata = get_metadata()
    # metadata_list = [{**metadata, **{'index': k}} for k in range(metadata['max_index']+1)]


    download_file.partial().expand(metadata = metadata)





