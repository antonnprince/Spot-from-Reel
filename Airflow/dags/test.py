from airflow import DAG
import requests
import json
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta


POSTGRES_CONN_ID = 'postgres_test'
API_CONN_ID = 'api_test'

default_args = {
    'owner':'airflow',
    'start_date':'2026-01-01',
}

# with DAG(dag_id = 'test_dag',default_args = default_args, schedule = "@daily", catchup = False) as dags:

#     @task()
#     def get_tasks():
#         print("At first task")
#         http_hook = HttpHook(http_conn_id = API_CONN_ID, method = 'GET')
#         endpoint = '/products?limit=10&skip=5&select=key1,key2'
#         response = http_hook.run(endpoint)
#         response.raise_for_status()
#         response = response.json()
#         print(f"received {len(response['products'])} products")
#         return response['products']
    
#     @task()
#     def insert_tasks(products):
#         print("At second task")
#         for product in products:
#             print(product)

#     first_task = get_tasks()
#     second_task = insert_tasks(first_task)


print(datetime.now())