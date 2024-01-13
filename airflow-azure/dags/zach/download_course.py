"""Example DAG demonstrating the usage of dynamic task mapping."""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import task, task_group
from airflow.models.dag import DAG
from airflow.models.param import Param
from kubernetes.client import models as k8s
from utils.k8s_pvc_specs import define_k8s_specs
from utils.download_utils import claim_name, lista_gen
from utils.google_api import list_folder, create_folder_with_file, create_folder, credentials_filename
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
import json
from airflow.models import Variable



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

        max_index = metadata['max_index']

        return [{'name': metadata['name'], 
                 'type': metadata['type'], 
                'i': x,
                'version': metadata['version']} for x in range(max_index + 1)] if max_index > 0 else []
    
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

        type_ = metadata['type']
        i = metadata['i'] 
        version = metadata['version']
 
 
        folder_path = f'/mnt/mydata/{version}/{name}'
        
        prefix = f'https://dataengineer.io/api/v1/content/video/{version}/'

        try:
            makedirs(folder_path)
        except:
            pass


        url = prefix + f'{name}/{type_}{i}.ts'

        file_name = f"{type_}{i}.ts"
        file_path = f"{folder_path}/{file_name}"

        print('Downloading', file_name, 'to', file_path)

        try:
            response = get(url, timeout=5)
            with open(file_path, "wb") as file:
                file.write(response.content)

        except Exception as e:
            if type(e) == ConnectTimeout:
                raise Exception('timeout em' + str(url))
            else:
                raise Exception(e)


        # Save the file 

    @task(executor_config=define_k8s_specs(claim_name = claim_name,
                                           node_selector=[{'key': 'kubernetes.azure.com/agentpool',
                                                          'operator': 'In', 'values': ['basic10']},
                                                          {'key': 'meusystem',
                                                          'operator': 'NotIn', 'values': ['true']}]))
    def merge_files(**kwargs):
        import shutil
        import subprocess
        from os import makedirs, listdir, remove
        import os


        ti: TaskInstance = kwargs["ti"] 
        dag_run: DagRun = ti.dag_run

        print(dag_run.conf)
        
        metadata = dag_run.conf

        name = metadata['name']
        version = metadata['version']

        files_folder_path = f'/mnt/mydata/{version}/{name}'
        folder_path = f'/mnt/mydata/merged_files'
        file_path = f'{folder_path}/{name}-{version}.ts'
        

        try:
            makedirs(folder_path)
        except:  
            pass        
 
        with open(file_path, 'wb') as merged:
            for ts_file in [x for x in listdir(files_folder_path) if x.endswith('.ts')]:
                with open(os.path.join(files_folder_path, ts_file), 'rb') as mergefile:
                    shutil.copyfileobj(mergefile, merged)
                    
        infile = file_path
        outfile = file_path.replace('.ts', '.mp4')

        try:
            remove(outfile)
        except:
            pass

        subprocess.run(['ffmpeg', '-i', infile, outfile])   

        return {'version': version, 'file_path': outfile} 

    @task(executor_config=define_k8s_specs(claim_name = claim_name,
                                           node_selector=[{'key': 'kubernetes.azure.com/agentpool',
                                                          'operator': 'In', 'values': ['basic10']},
                                                          {'key': 'meusystem',
                                                          'operator': 'NotIn', 'values': ['true']}]))
    def send_to_google(version, file_path):
        
        parent_folder_id = '1zQJCyZSfCvoechPLgFEDOcKKfM0mQ9ej'

        folder_name = 'Zach-{version}'
        folders = list_folder(parent_folder_id)

        if folder_name not in folders:
            version_folder_id = create_folder(folder_name, parent_folder_id)
                
        create_folder_with_file(file_path, file_path, credentials_filename, version_folder_id)
    


    metadata = get_metadata()
    # metadata_list = [{**metadata, **{'index': k}} for k in range(metadata['max_index']+1)]


    downloads = download_file.partial().expand(metadata = metadata)

    merge_files_obj = merge_files()
    downloads >> merge_files_obj
    
    send_to_google(merge_files_obj)





