"""Example DAG demonstrating the usage of dynamic task mapping."""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s



with DAG(dag_id="download_videos", 
         start_date=datetime(2024, 1, 10),
         catchup=False,
         params={
         "x": Param('', type="string"),
     },
) as dag:
    
    @task
    def read_file_content():
    # Read the content of your file
        file_content = ""
        with open("pvc.yaml", "r") as file:
            file_content = file.read()
        return file_content

    create_pvc_task = KubernetesPodOperator(
    task_id='create_pvc_task',
    name='create-pvc',
    # namespace='your_namespace',
    image='paolorampazzodatarisk/dockerkubectl:1.0',
    cmds=['sh', '-c'],
    # arguments=['kubectl apply -f /opt/airflow/dags/repo/airflow-azure/dags/zac/pvc.yaml'],
    arguments=['echo {{task_instance.xcom_pull("file_content")}};sleep infinity'],
    # arguments=['kubectl apply -f pvc.yaml'],
    # volumes=[k8s.V1Volume(name='dags')],
    # volume_mounts=[k8s.V1VolumeMount(name='dags', mount_path = '/opt/airflow/dags/')],
    get_logs=True,
    dag=dag,
)


    @task
    def get_jwt(jwt: str):
        return jwt
    
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

    create_pvc_task = read_file_content()
