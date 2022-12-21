import pymongo
import boto3
import os
from statistics import mode
import torch
import nltk
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from smart_open import open as smart_open
import io
from preprocess.preprocess_function import pre_process_tokenization_function
import __main__

# Define new directory to tmp directory
save_path = os.path.join('/tmp', 'mydir')
os.makedirs(save_path)
nltk.download('stopwords', download_dir=save_path)

# Stopwords
stopwords = open(os.path.join(save_path, "corpora/stopwords/english"), "r").read()
stopwords = stopwords.split("\n")
english_stop_words = [w for w in ENGLISH_STOP_WORDS]
stopwords.extend(["use", "uses", "used", "www", "gov",
                  "uk", "guidance", "pubns", "page"])
stopwords.extend(english_stop_words)


def download_sample_text(
        s3_client,
        bucket='beis-orp-dev-ingest',
        prefix='trigger-inference'):

    response = s3_client.list_objects_v2(
        Bucket=bucket, Prefix=prefix, StartAfter=prefix,)
    s3_files = response["Contents"]
    latest = max(s3_files, key=lambda x: x['LastModified'])
    file_content = s3_client.get_object(Bucket=bucket, Key=latest["Key"])[
        "Body"].read().decode('utf-8')
    return file_content


def download_model(
        s3_resource,
        bucket='beis-orp-dev-clustering-models',
        key='051222_torch.pt'):

    save_path = os.path.join('/tmp', 'modeldir')
    os.makedirs(save_path)
    s3_resource.Bucket(bucket).download_file(key, os.path.join(save_path, key))
    # Set prepreocess attribute
    setattr(
        __main__,
        "pre_process_tokenization_function",
        pre_process_tokenization_function)
    # Load the model in
    with smart_open(os.path.join(save_path, key), 'rb') as f:
        buffer = io.BytesIO(f.read())
        model = torch.load(buffer)
    # open(os.path.join(save_path, key), 'rb') as model_data:
    #     model = pickle.load(model_data)
        return model


def split_list(a, n):
    """
    Function to split the content into list of chunks of size n
    :param a: list of words
    :param n: size of the chunks
    """
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


def split_document_into_chunks(
        documents: str):

    extended_docs = []
    doclength = len(documents.split(" "))
    number_to_split_into = round(doclength / 4000 + 0.5)  # round up
    if number_to_split_into <= 1:
        extended_docs.append(documents)
    else:
        split_docs = list(split_list(documents, number_to_split_into))
        for num_docs in range(0, number_to_split_into):
            extended_docs.append(split_docs[num_docs])
    return extended_docs


def classify_data(model, input_data):
    split_document = split_document_into_chunks(input_data)
    topics = []
    for i in range(0, len(split_document)):
        topic = model.transform(split_document[i])
        topics.append(topic[0][0])
        # prediction = model(input_data)
    return mode(topics)


def handler(event, context):

    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]
    object_size = event["Records"][0]["s3"]["object"]["size"]

    print(f"New document in {bucket_name}: {object_key}, with size: {object_size}")

    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    metadata = s3_client.head_object(
        Bucket=bucket_name,
        Key=object_key
    )['Metadata']

    document_uid = metadata['uuid']

    print("Connecting to DocumentDB")
    db_client = pymongo.MongoClient(
        ("mongodb://ddbadmin:Test123456789@beis-orp-dev-beis-orp.cluster-cau6o2mf7iuc."
         "eu-west-2.docdb.amazonaws.com:27017/?directConnection=true"),
        tls=True,
        tlsCAFile="./rds-combined-ca-bundle.pem"
    )
    print("Connected to DocumentDB")

    # Define document database
    db = db_client.bre_orp
    collection = db.documents

    # download model
    model = download_model(s3_resource=s3_resource)

    # download text
    input_data = download_sample_text(s3_client=s3_client)

    # classify text
    topic = classify_data(model, input_data)

    # TODO: Change this to dict
    # Map names back onto topics
    if topic == 0:
        topic = "0_equipment_executive_exposure_assessment"
    if topic == 1:
        topic = "1_supplier_electricity_scheme_measure"
    if topic == 2:
        topic = "2_waste_soil_emission_monitoring"

    print(f"Topic predicted is: {topic}")

    # Insert document to DB
    print(collection.find_one({"document_uid": document_uid}))
    collection.find_one_and_update({"document_uid": document_uid}, {
                                   "$set": {"regulatory_topic": topic}})
    db_client.close()
    print("Topic updated in documentDB")

    return {
        "statusCode": 200,
        "document_uid": document_uid,
        "object_key": object_key
    }
