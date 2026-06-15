from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime,timedelta
import random

POSTGRES_CONN_ID = 'postgres_test'

default_args = {
    'owner':'airflow','start_date': datetime(2026,6,1)
}

with DAG(dag_id = 'sql_test', schedule = "@daily", default_args = default_args, catchup = False) as dags:

    @task
    def create_table():
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""CREATE TABLE IF NOT EXISTS test_for_airflow 
        (id INT PRIMARY KEY, 
        name TEXT,
        email TEXT
         )""")

        conn.commit()


    @task
    def insert_values(table_name, values, extra_queries):
        
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        num1 = random.randint(1,100)
        num2 = random.randint(5,100)

        columns = values.keys()

        cursor.execute(f"""
        INSERT INTO test_for_airflow VALUES({num1},'aaaaaa','BBBBBBB'),({num2},'b','cccccccccccccc')
        """)

        conn.commit()

    first = create_table()
    second = insert_values()