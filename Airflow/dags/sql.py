from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime,timedelta


POSTGRES_CONN_ID = 'postgres_test'

default_args = {
    'owner':'airflow','start_date': datetime.now() - timedelta(days=1)
}

with DAG(dag_id = 'sql_test', schedule = "@daily", default_args = default_args, catchup = False) as dags:

    @task
    def create_table():
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""CREATE TABLE test_for_airflow 
        (id INT PRIMARY KEY, 
        name TEXT,
        email TEXT
         )""")


    def insert_values():
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()


        cursor.execute("""
        INSERT INTO test_for_airflow VALUES(1,'aaaaaa','BBBBBBB'),(2,'b','cccccccccccccc')
        """)

        first = create_table()
        second = insert_values()