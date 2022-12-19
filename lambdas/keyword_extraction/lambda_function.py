import pymongo
import boto3
from keybert import KeyBERT
import wordninja
from sklearn.feature_extraction.text import CountVectorizer
import os
import re
from preprocess.preprocess_function import pre_process_tokenization_function
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
import nltk

# Define new directory to tmp directory
save_path = os.path.join('/tmp', 'mydir')
os.makedirs(save_path)
nltk.download('stopwords', download_dir = save_path)
nltk.download('punkt', download_dir = save_path)

# Stopwords
stopwords = open(os.path.join(save_path, "corpora/stopwords/english"), "r").read()
stopwords = stopwords.split("\n")
english_stop_words = [w for w in ENGLISH_STOP_WORDS]
stopwords.extend(["use", "uses", "used", "www", "gov",
                   "uk", "guidance", "pubns", "page"])
stopwords.extend(english_stop_words)

# Import KeyBERT
kw_model = KeyBERT()

# Vectorizer model
vectorizer_model= CountVectorizer(stop_words="english", tokenizer = pre_process_tokenization_function) # prevents noise and improves representation of clusters

# Define download text from bucket function
def download_sample_text(
        s3_client,
        bucket='beis-orp-dev-ingest',
        prefix='trigger-inference'):

        response = s3_client.list_objects_v2(
        Bucket=bucket, Prefix=prefix, StartAfter=prefix,)
        s3_files = response["Contents"]
        latest = max(s3_files, key=lambda x: x['LastModified'])
        file_content = s3_client.get_object(Bucket=bucket, Key=latest["Key"])["Body"].read().decode('utf-8') 
        return file_content

# Extract keywords
def extract_keywords(text):
    text = re.sub("Health and Safety Executive", "", text)
    text = re.sub("Ofgem", "", text)
    text = re.sub("Environmental Agency","", text)
    text = " ".join(wordninja.split(text))
    keywords = kw_model.extract_keywords(text, vectorizer = vectorizer_model, top_n = 10)
    keywords_list = [k[0] for k in keywords]
    return keywords_list


def handler(event, context):

    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]
    object_size = event["Records"][0]["s3"]["object"]["size"]

    print(f"New document in {bucket_name}: {object_key}, with size: {object_size}")

    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    doc_stream = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_key
    )['Body']

    metadata = s3_client.head_object(
        Bucket=bucket_name,
        Key=object_key
    )['Metadata']

    # uuid = metadata['uuid']
    test_uuid = "3d45dddd-0eae-401f-aaa2-1a0e3e93eece"

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
    
    # download text
    input_data = download_sample_text(
        s3_client=s3_client)

    # classify text
    keywords = extract_keywords(input_data)

    print(f"Keywords predicted are: {keywords}")

    # Insert document to DB 
    print(collection.find_one({"uuid": test_uuid}))
    collection.find_one_and_update({"uuid": test_uuid}, {"$set": {"keywords": keywords}})
    db_client.close()
    print("Keywords updated in documentDB")

    return {
        'statusCode': 200
        }
    

