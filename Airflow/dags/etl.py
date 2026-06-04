from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
# from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

import json
import requests

LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID= 'api_default'


default_args = {
    'owner': 'airflow',
    # 'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1), # start date is set to yesterday to allow immediate execution
    # 'retries': 1,
}


with DAG(dag_id='weather_etl_pipeline',default_args=default_args, schedule = "@daily", catchup=False) as dags:

    @task()
    def extract_weather_data():
        """Extract weather data from API"""
    
    http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET') #will get connection details from Airflow Connections
    
    # next we will build the endpoint
    # 
    endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
    # making the request
    # response = http_hook.run(endpoint)
    # response.raise_for_status() # will raise an error if the request was unsuccessful
    response = {"status":200,"latitude":51.5074,"longitude":-0.1278,"generationtime_ms":0.123,"utc_offset_seconds":0,"timezone":"GMT","timezone_abbreviation":"GMT","elevation":25.0,"current_weather":{"temperature":15.3,"windspeed":5.1,"winddirection":180,"weathercode":3,"time":"2024-06-01T12:00:00Z"}}
    weather_data = response # parse the response as JSON

    @task()
    def transform_weather_data(weather_data):
        """Transform weather data to fit the database schema"""
        transformed_data = {
            'latitude': weather_data['latitude'],
            'longitude': weather_data['longitude'],
            'temperature': weather_data['current_weather']['temperature'],
            'windspeed': weather_data['current_weather']['windspeed'],
            'winddirection': weather_data['current_weather']['winddirection'],
            'weathercode': weather_data['current_weather']['weathercode'],
            'time': weather_data['current_weather']['time']
        }
        return transformed_data

    @task()
    def load_weather_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID) # will get connection details from Airflow Connections
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(""" 
            CREATE TABLE IF NOT EXISTS weather_data(
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
        """)
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s);
        """, (transformed_data['latitude'], transformed_data['longitude'], transformed_data['temperature'],
        transformed_data['windspeed'], transformed_data['winddirection'], transformed_data['weathercode']))

        conn.commit()
        cursor.close()

    # ETL PIPELINE
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)