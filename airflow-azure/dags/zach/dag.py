"""Example DAG demonstrating the usage of dynamic task mapping."""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

with DAG(dag_id="download_videos", 
         start_date=datetime(2024, 1, 10),
         catchup=False,
         params={
         "x": Param('', type="string"),
     },
) as dag:

    @task
    def create_pvc():
        return 1

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
