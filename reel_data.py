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

response = get_response("HEYY LLama")

print(response)





