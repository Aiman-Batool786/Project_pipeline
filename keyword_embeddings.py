from openai import OpenAI
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

df = pd.read_csv("restricted_keywords_list.csv")

keywords = df["desc_and_spec_restricted_keywords"].tolist()

print("Generating embeddings in batch...")

response = client.embeddings.create(
    model="text-embedding-3-small",
    input=keywords
)

embeddings = [item.embedding for item in response.data]

df["embedding"] = embeddings

df.to_csv("keywords_with_embeddings.csv", index=False)

print("Done!")
