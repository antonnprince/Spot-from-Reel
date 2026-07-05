from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import time
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import json
import requests
from minio import Minio
from io import BytesIO

load_dotenv()
APIFY_TOKEN = os.getenv("APIFY_TOKEN")


POSTGRES_CONN_ID = 'reels_postgres' # this is the connection id we will use to connect to Postgres, it should be defined in Airflow Connections
API_CONN_ID= 'get_reels_api' 
DATASET_CONN_ID = 'reels_dataset'
RUN_STATUS_ID = 'reels_run_status'
GET_REEL_DATA = 'get_reel_data'

default_args = {
    'owner': 'airflow',
    'start_date':datetime(2025,10,3), # start date is set to yesterday to allow immediate execution
}


BODY = {
    "hashtags": [
        "kochifood",
        "cochinfood",
        "kochieats",
        "KochiEats",
        "KochiFood",
        "TasteKochi",
        "Ernakulam"
    ],
    "keywordSearch": True,
    "resultsLimit": 10,
    "resultsType": "reels"
}

        
client = Minio(
    "host.docker.internal:9000",
    access_key = "admin",
    secret_key="admin123",
    secure=False
)

buckets = ["run-metadata","reel-data"]

with DAG(dag_id='reel_ingestion_ppl',default_args=default_args, schedule = "@daily", catchup=False) as dags:
    
    def insert_values(schema_name,table_name, values, extra_queries = ""):
        
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        query_string = f"""
            INSERT INTO {schema_name}.{table_name}
            {   
                   '( '
                +' ,'.join([f" {key}" for key in values.keys()])
                +' )'
            }
             VALUES
            {
                '( ' + 
                ' ,'.join([
                    f" '{values[key]}'" for key in values.keys()
                ])
                + ' )'
            }
            """
        
        try:
            cursor.execute(query_string)
            conn.commit()
            print(f"Query for inserting values: {values} into table: {schema_name}.{table_name} with extra queries: {extra_queries}")
            print(f"Query is {query_string}")
            
        except Exception as e:
            raise e
        finally:
            cursor.close()
            conn.close()


    def create_table(schema_name:str,table_name:str, schema:dict):
        # schema -> {col_name, col_type}
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        
        query_string1 = f"""
            CREATE SCHEMA IF NOT EXISTS {schema_name}
            """
        query_string2 = f"""CREATE TABLE IF NOT EXISTS {table_name}
        {   '(' + 
            ', '.join([
                f"{key} {schema[key]}" for key in schema.keys() 
            ])
            + ')'
        }
            """
        
        try:
            cursor.execute(query_string1)
            cursor.execute(query_string2)
            conn.commit()
            print(f"Created schema {schema_name} and executed query {query_string2}")

        except Exception as e:
            conn.rollback()
            raise e

        finally:
            cursor.close()
            conn.close()

    @task
    def create_tables_in_pg():
        
        try:
            create_table("run_metadata","run_metadata.reels_actor_start_metadata",
            {
                "id": "TEXT",
                "actId": "TEXT",
                "userId": "TEXT",
                "startedAt": "TIMESTAMPTZ",
                "finishedAt": "TIMESTAMPTZ",
                "defaultDatasetId": "TEXT"
            })

            create_table("reel_data","reel_data.scraped_reels_data",{
                            "inputUrl": "TEXT",
                            "id": "TEXT",
                            "type": "TEXT",
                            "shortCode": "TEXT",
                            "caption":"TEXT",
                            "hashtags":"TEXT[]",
                            "mentions":"TEXT[]",
                            "url":"TEXT",
                            "commentsCount":"INT",
                            "firstComment":"TEXT",
                            "latestComments":"TEXT[]",
                            "dimensionsHeight":"INT",
                            "dimensionsWidth":"INT",
                            "displayUrl":"TEXT",
                            "locationName":"TEXT",
                            "images":"TEXT[]",
                            "videoUrl":"TEXT",
                            "likesCount":"INT",
                            "videoPlayCount":"INT",
                            "igPlayCounter":"INT",
                            "timestamp":"TIMESTAMPTZ",
                            "childPosts":"TEXT[]",
                            "ownerFullName":"TEXT",
                            "ownerUsername":"TEXT",
                            "ownerId":"TEXT",
                            "productType":"TEXT",
                            "videoDuration":"INT",
                            "musicInfo":"JSONB",
                            "audioUrl":"TEXT",
                            "alt":"TEXT",
                            "videoViewCount":"INT",
                            "isCommentsDisabled":"BOOLEAN",
                        })
        
        except Exception as e:
            print(f"Error creating tables: {e}")
            raise e
        finally:
            print("Finished creating tables if not exist.")

    @task
    def create_buckets():
        for bucket in buckets:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)

    @task
    def start_reels_scraper_actor():
        print("Starting the reels scraper actor...")

        try:
            # http_hook = HttpHook(http_conn_id = API_CONN_ID, method = 'POST')
            # endpoint = f"/v2/acts/apify~instagram-hashtag-scraper/runs?token={APIFY_TOKEN}"
            # response = http_hook.run(endpoint, json=BODY)
            # response.raise_for_status()
            # print(f"API response: {response.json()}")
            
            # for i in response.json()["data"]:
            #     if i in ["id", "actId", "userId", "startedAt", "finishedAt","defaultDatasetId"]:
            #         print(f"{i}: {response.json()['data'][i]}")
            #         resultMetadata[i] = response.json()['data'][i]

            current_date = datetime.now()
            
            response = {
                    "id": "kYpwjE8IgWafp4ndc",
                    "actId": "reGe1ST3OBgYZSsZJ",
                    "userId": "EqSYJcIUkn36T4T5R",
                    "startedAt": "2026-05-20T06:02:30.506Z",
                    "finishedAt": "2026-05-20T06:02:30.506Z",
                    "defaultDatasetId": "AZzRj2eUOGMqs63hx"
            }
            
            run_metadata_json = json.dumps(response)
            
            client.put_object(
                bucket_name = "run-metadata",
                object_name = f"actor_start_{current_date}",
                data=BytesIO(run_metadata_json.encode("utf-8")),
                length=len(run_metadata_json.encode("utf-8")),
                content_type="application/json"
            )
            
            resultMetadata = response
            
            insert_values("run_metadata","reels_actor_start_metadata", resultMetadata, extra_queries = "ON CONFLICT DO NOTHING")
            
            return resultMetadata

        except Exception as e:
            raise e

        finally:
            print("Fetched actoor metadata and inserted values")

    @task
    def check_run_status(resultMetadata):
        
        print("Getting dataset id...")

        time.sleep(10)
        
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        # cursor = conn.cursor()
  
        # status_url = f"https://api.apify.com/v2/actor-runs/{resultMetadata['id']}?token={APIFY_TOKEN}"
        
        while True:
            try:
                # status_response = requests.get(status_url)
                
                # status_response.raise_for_status()
                # status_response = status_response.json()
                # print(f"Status response: {status_response}")
            
                # status = status_response["data"]["status"]
                status = "SUCCEEDED"
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

    @task
    def fetch_data(dataset_id):

        try:

            # http_hook = HttpHook(http_conn_id = API_CONN_ID, method = 'GET' )
            # endpoint = f"/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}"
            # reel_data = http_hook.run(endpoint)
            # reel_data.raise_for_status()
            # reel_data = reel_data.json()
                    
            
            print("========going to load data from json=============")
            
            with open("/usr/local/airflow/include/scraped_instagram_reel_data.json","r",encoding = "utf-8") as f:
                reel_data = json.load(f)

            current_date = datetime.now()

            reel_data_bytes = json.dumps(reel_data, indent = 4).encode("utf-8")
                        
            client.put_object(
            bucket_name = "reel-data",
            object_name = f"scraped_reel_data_{current_date}",
            data=BytesIO(reel_data_bytes),
            length=len(reel_data_bytes),
            content_type="application/json"
            )
        
            print("============Loaded scraped instagram data from json and pushed to minio========")
        
        except Exception as e:
            raise e


    create_tables = create_tables_in_pg()
    create_bucket = create_buckets()
    start_reel_scraper = start_reels_scraper_actor()
    dataset_id = check_run_status(start_reel_scraper)
    fetching_data = fetch_data(dataset_id)
