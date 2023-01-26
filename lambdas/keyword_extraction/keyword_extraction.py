import pymongo
import boto3
import wordninja
from sklearn.feature_extraction.text import CountVectorizer
import os
import re
import nltk
from smart_open import open as smart_open
import torch
import io
from nltk.tokenize import word_tokenize
from bs4 import BeautifulSoup
from nltk.stem import WordNetLemmatizer
import zipfile


SOURCE_BUCKET = "beis-orp-dev-datalake"

print('Initiating WordNetLemmatizer')
wnl = WordNetLemmatizer()
print('Initiated WordNetLemmatizer')

# Define new directory to tmp directory
save_path = os.path.join('/tmp', 'nltk_data')
os.makedirs(save_path, exist_ok=True)
nltk.download('wordnet', download_dir=save_path)
nltk.download('omw-1.4', download_dir=save_path)
nltk.download('punkt', download_dir=save_path)

# Unzip all resources
with zipfile.ZipFile(os.path.join(save_path, "corpora", "wordnet.zip"), 'r') as zip_ref:
    zip_ref.extractall(os.path.join(save_path, "corpora"))
with zipfile.ZipFile(os.path.join(save_path, "corpora", "omw-1.4.zip"), 'r') as zip_ref:
    zip_ref.extractall(os.path.join(save_path, "corpora"))
with zipfile.ZipFile(os.path.join(save_path, "tokenizers", "punkt.zip"), 'r') as zip_ref:
    zip_ref.extractall(os.path.join(save_path, "tokenizers"))

print(os.listdir(save_path))

stopwords = open("./stopwords.txt", "r")
stopwords = stopwords.read()
stopwords = [i for i in stopwords.split("\n")]
stopwords.extend(["use", "uses", "used", "www", "gov",
                  "uk", "guidance", "pubns", "page"])


def download_model(
        s3_resource,
        bucket='beis-orp-dev-clustering-models',
        key='keybert.pt'):

    save_path = os.path.join('/tmp', 'modeldir')
    os.makedirs(save_path, exist_ok=True)
    s3_resource.Bucket(bucket).download_file(key, os.path.join(save_path, key))
    # Load the model in
    with smart_open(os.path.join(save_path, key), 'rb') as f:
        buffer = io.BytesIO(f.read())
        model = torch.load(buffer)
        return model


def pre_process_tokenization_function(
        documents: str,
        stop_words=stopwords,
        wnl=wnl):

    # Preprocess data after embeddings are created
    text = BeautifulSoup(documents).get_text()
    # fetch alphabetic characters
    text = re.sub("[^a-zA-Z]", " ", text)
    # define stopwords
    remove_stop_words = set(stop_words)
    # lowercase
    text = text.lower()
    # tokenize
    word_tokens = word_tokenize(text)
    filtered_sentence = []
    for w in word_tokens:
        if w not in remove_stop_words:
            filtered_sentence.append(w)
    # Remove any small characters remaining
    filtered_sentence = [word for word in filtered_sentence if len(word) > 1]
    # Lemmatise text
    lemmatised_sentence = [wnl.lemmatize(word) for word in filtered_sentence]
    return lemmatised_sentence


# Vectorizer model
# prevents noise and improves representation of clusters
vectorizer_model = CountVectorizer(
    stop_words="english",
    tokenizer=pre_process_tokenization_function)


def extract_keywords(text, kw_model):
    text = re.sub("Health and Safety Executive", "", text)
    text = re.sub("Ofgem", "", text)
    text = re.sub("Environmental Agency", "", text)
    text = " ".join(wordninja.split(text))
    keywords = kw_model.extract_keywords(
        text, vectorizer=vectorizer_model, top_n=10)
    return keywords


def handler(event, context):

    print(f"Event received: {event}")
    document_uid = event["document_uid"]

    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    # Download text from Data Lake
    doc_stream = s3_client.get_object(
        Bucket=SOURCE_BUCKET,
        Key=f"processed/{document_uid}.txt"
    )['Body'].read().decode("utf-8")

    # Classify Text
    kw_model = download_model(s3_resource)
    keywords = extract_keywords(doc_stream, kw_model)
    subject_keywords = [i[0] for i in keywords]

    print(f"Keywords predicted are: {keywords}")

    # Connect to documentDB
    db_client = pymongo.MongoClient(
        ("mongodb://ddbadmin:Test123456789@beis-orp-dev-beis-orp.cluster-cau6o2mf7iuc."
         "eu-west-2.docdb.amazonaws.com:27017/?directConnection=true"),
        tls=True,
        tlsCAFile="./rds-combined-ca-bundle.pem"
    )
    print(db_client.list_database_names())
    print("Connected to DocumentDB")

    # Define document database
    db = db_client.bre_orp
    collection = db.documents

    # Insert document to DB
    print(collection.find_one({"document_uid": document_uid}))
    collection.find_one_and_update({"document_uid": document_uid}, {
                                   "$set": {"subject_keywords": subject_keywords}})
    db_client.close()
    print("Keywords updated in documentDB")

    return {
        "statusCode": 200,
        "document_uid": document_uid
    }
