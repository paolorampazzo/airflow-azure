"""Example DAG demonstrating the usage of dynamic task mapping."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import task, task_group
from airflow.models.dag import DAG
from airflow.models.param import Param
from kubernetes.client import models as k8s
from utils.k8s_pvc_specs import define_k8s_specs
from utils.download_utils import claim_name, lista_gen
from utils.google_api import list_folder, create_folder_with_file, create_folder, send_to_drive
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy_operator import DummyOperator
import json
from airflow.models import Variable

PARENT_FOLDER_ID = '1zQJCyZSfCvoechPLgFEDOcKKfM0mQ9ej'


with DAG(dag_id="download_course", 
         start_date=datetime(2024, 1, 10),
         catchup=False,
         max_active_runs = 50,
         max_active_tasks = 200,
) as dag:
    
    @task()
    def get_metadata(**kwargs):
        ti: TaskInstance = kwargs["ti"] 
        dag_run: DagRun = ti.dag_run

        print(dag_run.conf)
        
        metadata = dag_run.conf

        max_index = metadata['max_index']
        max_index = min(max_index, 50)

        if max_index == -1:
            max_index = 0

        return [{'name': metadata['name'], 
                 'type': metadata['type'], 
                'i': x,
                'version': metadata['version'], 'error': True if max_index == 0 else False} for x in range(max_index + 1)] 
    
    @task(retries=3, retry_delay = timedelta(seconds=5),
          executor_config=define_k8s_specs(claim_name = claim_name,
                                           node_selector=[{'key': 'kubernetes.azure.com/agentpool',
                                                          'operator': 'NotIn', 'values': ['paolo1']},
                                                          {'key': 'meusystem',
                                                          'operator': 'NotIn', 'values': ['true']}]))
    def download_file(metadata):
        from requests import get
        from requests.exceptions import ConnectTimeout
        from os import makedirs
        import os
        
        name = metadata['name'] 

        type_ = metadata['type']
        i = metadata['i'] 
        version = metadata['version']
        error = metadata['error']

        if error:
        
            parent_folder_id = PARENT_FOLDER_ID
            folder_path = f'/mnt/mydata/merged_files'
            filename = {name}-{version}.txt
            file_path = f'{folder_path}/{filename}'

            with open(file_path, 'w') as f:
                for line in ['Error']:
                    f.write(f"{line}\n")
                    
            send_to_drive(version, name, parent_folder_id, file_path, filename=filename, overwrite=True)
            os.remove(file_path)

            return
            
 
 
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

    @task.branch(executor_config=define_k8s_specs(claim_name = claim_name,
                                           node_selector=[{'key': 'kubernetes.azure.com/agentpool',
                                                          'operator': 'NotIn', 'values': ['paolo1']},
                                                          {'key': 'meusystem',
                                                          'operator': 'NotIn', 'values': ['true']}]))
    def filter_errors(**kwargs):
        ti: TaskInstance = kwargs["ti"] 
        dag_run: DagRun = ti.dag_run

        print(dag_run.conf)
        
        metadata = dag_run.conf

        error = metadata['max_index'] == -1

        if not error:
            return 'merge_files'
        
        return 'finish'
                
    

    @task(executor_config=define_k8s_specs(claim_name = claim_name,
                                           node_selector=[{'key': 'kubernetes.azure.com/agentpool',
                                                          'operator': 'NotIn', 'values': ['paolo1']},
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

        return {'version': version, 'file_path': outfile, 'name': name} 

    @task(executor_config=define_k8s_specs(claim_name = claim_name,
                                           node_selector=[{'key': 'kubernetes.azure.com/agentpool',
                                                          'operator': 'NotIn', 'values': ['paolo1']},
                                                          {'key': 'meusystem',
                                                          'operator': 'NotIn', 'values': ['true']}]))
    def send_to_google(data):
        version, file_path, name = data['version'], data['file_path'], data['name']
        
        parent_folder_id = PARENT_FOLDER_ID
        
        filename = '-'.join([name, version]) + '.mp4'
        send_to_drive(version, name, parent_folder_id, file_path, filename, overwrite=True)

        


    metadata = get_metadata()
    # metadata_list = [{**metadata, **{'index': k}} for k in range(metadata['max_index']+1)]

    @task(executor_config=define_k8s_specs(claim_name = claim_name,
                                           node_selector=[{'key': 'kubernetes.azure.com/agentpool',
                                                          'operator': 'NotIn', 'values': ['paolo1']},
                                                          {'key': 'meusystem',
                                                          'operator': 'NotIn', 'values': ['true']}]))
    def delete_files(**kwargs):
        import os
        ti: TaskInstance = kwargs["ti"] 
        dag_run: DagRun = ti.dag_run

        print(dag_run.conf)
        
        metadata = dag_run.conf

        name = metadata['name']
        version = metadata['version']

        files_folder_path = f'/mnt/mydata/{version}/{name}'
        folder_path = f'/mnt/mydata/merged_files'
        merged_folder = f'{folder_path}/{name}-{version}'
        # file_paths = [merged_folder + x for x in ['.ts', '.mp4']]

        for file in os.listdir(files_folder_path):
            os.remove(file)

        for file in os.listdir(merged_folder):
            os.remove(file)

        

    finish = DummyOperator(task_id="finish")

    downloads = download_file.partial().expand(metadata = metadata)

    merge_files_obj = merge_files()
    filter_errors_obj = filter_errors()
    filter_errors_obj >> merge_files_obj
    filter_errors_obj >> finish
    downloads >> filter_errors_obj
    send_to_google(merge_files_obj) >> delete_files()





