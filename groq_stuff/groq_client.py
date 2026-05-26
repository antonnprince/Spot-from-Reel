from groq import Groq
from dotenv import load_dotenv
import os
import json 

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# def get_response(posts):
    # response = client.chat.completions.create(
    #     model="llama-3.3-70b-versatile",
    #     messages=[
    #         {
    #             "role": "system",
    #             "content": (
    #                 "You are a data extraction assistant. "
    #                 "You ONLY respond with valid raw JSON. "
    #                 "No markdown, no code blocks, no explanation. "
    #                 "Just the JSON object or array."
    #             )
    #         },
    #         {
    #             "role": "user",
    #             "content": (
    #                 "From the Instagram posts below, extract cafe/restaurant names and locations. "
    #                 "Return a JSON array where each item has 'name' and 'location' fields only.\n\n"
    #                 f"{json.dumps(posts, ensure_ascii=False)}"
    #             )
    #         }
    #     ],
    #     temperature=0,          # deterministic output for structured data
    #     max_completion_tokens=4096,  # increased to avoid truncation
    #     top_p=1,
    #     stream=False,
    #     stop=None
    # )

    # raw = response.choices[0].message.content.strip()

    # # Strip markdown fences if model still adds them
    # if "```" in raw:
    #     raw = raw.split("```")[1]
    #     if raw.startswith("json"):
    #         raw = raw[4:]
    #     raw = raw.strip()

    # # Safe parse with detailed error output
    # try:
    #     result = json.loads(raw)
    #     # print(json.dumps(result, indent=2, ensure_ascii=False))
    #     return result
    # except json.JSONDecodeError as e:
    #     print(f"JSON parse failed: {e}")
    #     print(f"Raw output was:\n{raw}")
    #     return []


def get_response(posts, batch_size=10):
    all_results = []
    
    for i in range(0, len(posts), batch_size):
        batch = posts[i:i+batch_size]
        print(f"Processing batch {i//batch_size + 1} ({len(batch)} posts)...")
        
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a data extraction assistant. "
                        "You ONLY respond with valid raw JSON. "
                        "No markdown, no code blocks, no explanation."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        "From the Instagram posts below, extract cafe/restaurant names and locations. "
                        "Return a JSON array where each item has 'id','name' and 'location' fields only. If any field is missing, use null. Do not leave out any items.\n\n"
                        f"{json.dumps(batch, ensure_ascii=False)}"
                    )
                }
            ],
            temperature=0,
            max_completion_tokens=4096,
            stream=False
        )

        raw = response.choices[0].message.content.strip()

        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        try:
            result = json.loads(raw)
            all_results.extend(result)
        except json.JSONDecodeError as e:
            print(f"Batch {i//batch_size + 1} parse failed: {e}")
            print(f"Raw: {raw}")

    # print(json.dumps(all_results, indent=2, ensure_ascii=False))
    return all_results