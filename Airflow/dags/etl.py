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
POSTGRES_CONN_ID = 'reels_postgres' # this is the connection id we will use to connect to Postgres, it should be defined in Airflow Connections
API_CONN_ID= 'get_reels_api'
DATASET_CONN_ID = 'reels_dataset'
RUN_STATUS_ID = 'reels_run_status'

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1), # start date is set to yesterday to allow immediate execution
}


BODY = {
    "hashtags": [
        "kochifood",
        "cochinfood",
        "kochieats",
        "KochiEats",
        "KochiFood",
        "TasteKochi"
    ],
    "keywordSearch": True,
    "resultsLimit": 10,
    "resultsType": "reels"
}

with DAG(dag_id='weather_etl_pipeline',default_args=default_args, schedule = "@daily", catchup=False) as dags:

    @task
    def start_reels_scraper_actor():
        http_hook = HttpHook(http_conn_id = API_CONN_ID, method = 'POST')
        endpoint = f"/v2/acts/apify~instagram-hashtag-scraper/runs?token={APIFY_TOKEN}"
        response = http_hook.run(endpoint, json=BODY)
        response.raise_for_status()
        print(f"API response: {response.json()}")
        resultMetadata = {}
        for i in response.json()["data"]:
            if i in ["id", "actId", "userId", "startedAt", "finishedAt","defaultDatasetId"]:
                print(f"{i}: {response.json()['data'][i]}")
                resultMetadata[i] = response.json()['data'][i]
        return resultMetadata

    @task()
    def get_dataset_id(resultMetadata):
        time.sleep(10)

        status_url = f"https://api.apify.com/v2/actor-runs/{resultMetadata['id']}?token={APIFY_TOKEN}"
        
        while True:
            try:
                status_response = requests.get(status_url)
                
                status_response.raise_for_status()
                status_response = status_response.json()
                print(f"Status response: {status_response}")
            
                status = status_response["data"]["status"]

                if status == "SUCCEEDED":
                    print("====================DONE=======================")
                    return resultMetadata['defaultDatasetId']
                
                elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
                    print(f"===============STATUS IS {status} =======================")
                    return None

                print(f"Current status: {status}. Checking again in 15 seconds...")
                time.sleep(15)  

            
            except requests.RequestException as e:
                print(f"Error occurred: {e}")
                time.sleep(10)
                break
