# import json
# import requests


# with open("result_from_llm.json","r") as f:
#     raw = json.load(f)


# with open("scraped_data_instagram_hashtag_scraper.json","r") as f:
#     master_data = json.load(f)



# print(f"Count of response from llm is  {len(raw)}")
# print(f"Count of master data is {len(master_data)}")

# master_ids = {item.get("id") for item in master_data}
# raw_ids = { item.get("id") for item in raw}


# # for item in raw:
# #     if item.get("id") in master_ids:
# #         print(f"Found match for id: {item.get('id')}")
# #     else:
# #         print(f"No match found for id: {item.get('id')}")

# raw_ids = set(raw_ids)
# master_ids = set(master_ids)
# print(f"IDs in raw but not in master: {master_ids - raw_ids }")
# print(f"Count of raw id: {len(raw_ids)} and count of master id: {len(master_ids)}")

def insert_values(table_name, schema, extra_queries = ""):

        return "someting"
            

some_values = {"key1": "TEXT", "key2": "INT", "key3": "TEXT"}
print(insert_values("my_table", some_values, extra_queries = "ON CONFLICT DO NOTHING"))
            # """)