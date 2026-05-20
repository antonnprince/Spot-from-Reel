import requests
import time
import json
import os
from dotenv import load_dotenv

load_dotenv()

ACTOR_ID = "apify~instagram-hashtag-scraper"
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

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

RESULTS_LIMIT = 20

headers = {
    "Content-Type":"application/json"
}

res = requests.post(f"https://api.apify.com/v2/acts/apify~instagram-hashtag-scraper/runs?token={APIFY_TOKEN}",
                    json = BODY,
                    headers = headers
                    )


res = res.json()


resultMetadata = {}

for i in res["data"]:
    if i in ["id", "actId", "userId", "startedAt", "finishedAt","defaultDatasetId"]:
        resultMetadata[i] = res['data'][i]
 
time.sleep(10)

print(f"Dataset id: {resultMetadata['defaultDatasetId']}")
scrapedData = requests.get(f"https://api.apify.com/v2/datasets/{resultMetadata['defaultDatasetId']}/items?token={APIFY_TOKEN}")

with open("scraped_data_instagram_hashtag_scraper.json", "w") as f:
    json.dump(scrapedData.json(), f, indent=4)

