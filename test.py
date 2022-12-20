import os
from keybert import KeyBERT


# Define keybert model
model_path = os.path.join('/tmp', 'modeldir')
os.makedirs(model_path)
with open(model_path + "/kw_model", "r") as kw_model:
    kw_model = KeyBERT()