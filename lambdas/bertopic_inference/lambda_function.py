import bertopic
from nltk.tokenize import word_tokenize
import boto3
import os
from statistics import mode
import torch
import pickle
import nltk
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
from smart_open import open as smart_open
import io
from preprocess.preprocess_function import pre_process_tokenization_function
import __main__


# Change directory to tmp directory
save_path = os.path.join('/tmp', 'mydir')
os.makedirs(save_path)
nltk.download('stopwords', download_dir = save_path)

# Stopwords
stopwords = open(os.path.join(save_path, "corpora/stopwords/english"), "r").read()
stopwords = stopwords.split("\n")
english_stop_words = [w for w in ENGLISH_STOP_WORDS]
stopwords.extend(["use", "uses", "used", "www", "gov",
                   "uk", "guidance", "pubns", "page"])
stopwords.extend(english_stop_words)

# Define download text from bucket function
def download_sample_text(
        s3_client,
        bucket='beis-orp-dev-ingest',
        key='trigger-inference'):

    sample_text = s3_client.get_object(bucket, key)['Body'].read()
    return sample_text


def download_model(
        s3_resource,
        bucket='beis-orp-dev-clustering-models',
        key='051222_torch.pt'):

    save_path = os.path.join('/tmp', 'modeldir')
    os.makedirs(save_path)
    s3_resource.Bucket(bucket).download_file(key, os.path.join(save_path, key))
    # Set prepreocess attribute
    setattr(__main__, "pre_process_tokenization_function", pre_process_tokenization_function)
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
        self,
        documents: str):

    extended_docs = []
    doclength = len(word_tokenize(documents))
    number_to_split_into = round(doclength / 4000 + 0.5)  # round up
    if number_to_split_into <= 1:
        extended_docs.append(documents)
    else:
        split_docs = list(self.split_list(documents, number_to_split_into))
        for num_docs in range(0, number_to_split_into):
            extended_docs.append(split_docs[num_docs])
    return extended_docs


def classify_data(model, input_data):
    split_document = split_document_into_chunks(input_data)
    topics = []
    for i in range(0, len(split_document)):
        topic = model.transform(split_document[i])
        topics.append(topic)
        # prediction = model(input_data)
    return mode(topics)


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

    doc_bytes = doc_stream.read()
    doc_text = doc_bytes.decode('utf8')
    # uuid = metadata['uuid']

    # print(f"Document text: {doc_text}")
    # print(f"UUID obtained is: {uuid}")

    # download model
    model = download_model(s3_resource = s3_resource)
    
    # download text
    input_data = download_sample_text(
        s3_client=s3_client)

    # classify text
    topic = classify_data(model, input_data)
    if topic:
        return {
            'statusCode': 200,
            'class': topic
        }
    else:
        return {
            'statusCode': 404,
            'class': None
        }
