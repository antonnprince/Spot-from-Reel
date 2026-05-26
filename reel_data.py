import json
from dotenv import load_dotenv
import os
import re
from groq_stuff.groq_client import get_response



FIELDS = {
   "id", "caption", "hashtags", "mentions", "url", "displayUrl",
    "videoUrl", "likesCount", "timestamp", "locationName",
    "ownerFullName", "ownerUsername", "musicInfo", "taggedUsers",
    "coauthorProducers", "audioUrl"
}

FIELDS_FOR_MODEL = {
    "id","caption", "mentions", "locationName","coauthorProducers"
}

def remove_emojis(text):
    return re.sub(r'[^\x00-\x7F]+', '', text).strip()

with open("scraped_data_instagram_hashtag_scraper.json", "r", encoding="utf-8") as f:
    raw = json.load(f)

filtered_data = []

for post in raw:
    extracted_post = {key: post.get(key) for key in FIELDS}
    filtered_data.append(extracted_post)

data_for_model = []
coauthor_producers =""

for post in filtered_data:
    entry = {key: post.get(key, "") for key in FIELDS_FOR_MODEL if key != "coauthorProducers"}
    entry["caption"] = remove_emojis(entry.get("caption", ""))
    coauthors = post.get("coauthorProducers") or []  
    entry["coauthorProducers"] = [c.get("full_name") for c in coauthors if c.get("full_name")]
    
    data_for_model.append(entry)


# print(json.dumps(data_for_model, indent=4))

response = get_response(data_for_model)
print(f"Count is {len(response)}")


for item in response:
    item["location"] = "Kochi" if item.get("location") == None else item.get("location")


with open("result_from_llm.json","w") as f:
    json.dump(response,f,indent=4,ensure_ascii=False)






