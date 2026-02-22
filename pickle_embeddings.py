import pandas as pd
import numpy as np
import pickle
import ast

print(" Loading categories_with_embeddings.csv ...")
df = pd.read_csv("categories_with_embeddings.csv")

print(" Converting embedding strings to numpy arrays ...")
embeddings = df["embedding"].apply(lambda x: np.array(ast.literal_eval(x)))

print(" Saving category_embeddings.pkl ...")
with open("category_embeddings.pkl", "wb") as f:
    pickle.dump(np.stack(embeddings.values), f)

print(" Pickle file created successfully!")
