import pandas as pd
import numpy as np
import pickle
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

print(" Loading categories...")
df = pd.read_csv("categories.csv", encoding="latin1")

def get_embedding(text: str):
    response = client.embeddings.create(
        input=text,
        model="text-embedding-3-small"
    )
    return np.array(response.data[0].embedding)

print("Generating embeddings (one-time process)...")
df["embedding"] = df["category_text"].apply(get_embedding)
# df["embedding"] = df["category_name"].apply(get_embedding)

# Save CSV (human readable backup)
df.to_csv("categories_with_embeddings.csv", index=False)

# SAVE PICKLE (binary)
with open("category_embeddings.pkl", "wb") as f:
    pickle.dump(np.stack(df["embedding"].values), f)

print(" Done! Created:")
print("   • categories_with_embeddings.csv")
print("   • category_embeddings.pkl")
