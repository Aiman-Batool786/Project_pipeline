import sqlite3
import numpy as np
import pickle
from openai import OpenAI
import os
from dotenv import load_dotenv
from sklearn.metrics.pairwise import cosine_similarity


load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

DB_NAME = "products.db"


# Load categories from database
def load_categories():

    conn = sqlite3.connect(DB_NAME)

    cursor = conn.cursor()

    cursor.execute("""

        SELECT category_id, category_name, embedding
        FROM categories

    """)

    rows = cursor.fetchall()

    conn.close()

    category_ids = []
    category_names = []
    embeddings = []

    for row in rows:

        category_ids.append(row[0])

        category_names.append(row[1])

        embeddings.append(pickle.loads(row[2]))

    return category_ids, category_names, np.array(embeddings)


category_ids, category_names, category_embeddings = load_categories()



def get_embedding(text):

    response = client.embeddings.create(

        model="text-embedding-3-small",
        input=text

    )

    return np.array(response.data[0].embedding)



def assign_category(title, description):

    product_text = title + " " + description

    product_embedding = get_embedding(product_text)

    sims = cosine_similarity(

        [product_embedding],
        category_embeddings

    )[0]

    idx = sims.argmax()

    return {

        "category_id": category_ids[idx],
        "category_name": category_names[idx],
        "confidence": float(sims[idx])

    }



# import pandas as pd
# import numpy as np
# import pickle
# from openai import OpenAI
# import os
# from dotenv import load_dotenv
# from sklearn.metrics.pairwise import cosine_similarity


# load_dotenv()

# client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# # Load categories

# df = pd.read_csv("categories_with_embeddings.csv")


# # Load embeddings from pickle

# with open("category_embeddings.pkl", "rb") as f:

#     category_embeddings = pickle.load(f)



# def get_embedding(text):

#     response = client.embeddings.create(

#         model="text-embedding-3-small",

#         input=text

#     )

#     return np.array(response.data[0].embedding)



# def assign_category(title, description):

#     product_text = title + " " + description


#     product_embedding = get_embedding(product_text)


#     sims = cosine_similarity(

#         [product_embedding],

#         category_embeddings

#     )[0]


#     idx = sims.argmax()


#     return {

#         "category_id": int(df.iloc[idx]["category_id"]),

#         "category_name": df.iloc[idx]["category_name"],

#         "confidence": float(sims[idx])

#     }
