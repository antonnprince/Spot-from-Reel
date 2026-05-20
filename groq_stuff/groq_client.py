from groq import Groq
from dotenv import load_dotenv
import os

load_dotenv()

client = Groq(api_key = os.getenv("GROQ_API_KEY"))

def get_response(prompt, stream=False):

  completion = client.chat.completions.create(
      model="llama-3.1-8b-instant",
      messages=[
        {
          "role": "user",
          "content": prompt
        }
      ],
      temperature=1,
      max_completion_tokens=1024,
      top_p=1,
      stream=stream,
      stop=None
  )

  if stream:
    full_response = ""

    for chunk in completion:
        content = chunk.choices[0].delta.content or ""

        print(content, end="")
        full_response += content

    return full_response

    # Normal response
  return completion.choices[0].message.content

    