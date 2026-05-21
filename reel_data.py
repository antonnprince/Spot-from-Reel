import json
from dotenv import load_dotenv
import os
from groq_stuff.groq_client import get_response



FIELDS = {
    "caption", "hashtags", "mentions", "url", "displayUrl",
    "videoUrl", "likesCount", "timestamp", "locationName",
    "ownerFullName", "ownerUsername", "musicInfo", "taggedUsers",
    "coauthorProducers", "audioUrl"
}


with open("scraped_data_instagram_hashtag_scraper.json", "r", encoding="utf-8") as f:
    raw = json.load(f)

filtered_data = []

for post in raw:
    extracted_post = {key: post.get(key) for key in FIELDS}
    filtered_data.append(extracted_post)


# print(json.dumps(filtered_data[0:3], indent=4))

response = get_response(f"I want you to find the cafe name and location from the following Instagram reel data. Return in json format of {'name':XXXX,'location':XXXX}. If there is no location specified, return default location value as 'Kochi'.")

print(response)





