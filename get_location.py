import json
import requests
import dotenv
import time
import os

dotenv.load_dotenv()

headers = {
    "Content-Type":"application/json"
}

APIFY_TOKEN = os.getenv("APIFY_TOKEN")

ACTOR_ID = "solidcode~google-maps-scraper-2-5-per-1-000-results"

with open("result_from_llm.json","r") as f:
    reel_data = json.load(f)

print("Ingested reel data from json file")

reel_data = reel_data[0:5]

location_result = []

payload_array = [
    {
        "id": item["id"],
        "searchQuery": item["name"] + " " + item["location"]
    }
    for item in reel_data]

print(f"payload_array: {payload_array}")

for location in payload_array:

    start_url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs?token={APIFY_TOKEN}"

    payload = {
        "concurrentSearches": 1,
        "countryCode": "IN",
        "language": "en",
        "locationName": "Kochi, Kerala",
        "maxResults": 5,
        "searchQueries": [
            location["searchQuery"]
        ]
    }

    response = requests.post(start_url, json=payload)
    
    response.raise_for_status()

    run_data = response.json()["data"]
    
    run_id = run_data["id"]
    
    dataset_id = run_data["defaultDatasetId"]

    print(f"Run ID:     {run_id}")

    print(f"Dataset ID: {dataset_id}")

    print("\nWaiting for run to complete...")

    status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"

    while True:
        status_resp = requests.get(status_url)
        status = status_resp.json()["data"]["status"]

        print(f"  Status: {status}")

        if status == "SUCCEEDED":
            print("Done!")
            break
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise Exception(f"Run failed with status: {status}")

        time.sleep(15)  # Google Maps takes longer, poll every 15s

    
    print("\nFetching results...")

    dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}"

    results = requests.get(dataset_url).json()
    
    # results["reel_id"] = location["id"]
    
    location_result.append({
        "reel_id":location["id"],
        "data":results,
        "count":len(results)
    })

with open("location.json","w") as f:
    json.dump(location_result,f,indent = 4)

print(f"Total places found: {len(location_result)}")




