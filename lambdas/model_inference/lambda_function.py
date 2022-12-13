from nltk.corpus import stopwords
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS
import nltk
import boto3
import os
from statistics import mode
from nltk.tokenize import word_tokenize
import torch
from bs4 import BeautifulSoup
import re
from nltk.stem import WordNetLemmatizer


wnl = WordNetLemmatizer()
nltk.download('stopwords')

# Stopwords
stop_words = stopwords.words('english')
english_stop_words = [w for w in ENGLISH_STOP_WORDS]
stop_words.extend(["use", "uses", "used", "www", "gov",
                   "uk", "guidance", "pubns", "page"])
stop_words.extend(english_stop_words)


def pre_process_tokenization_function(
        documents: str,
        stop_words,
        wnl):

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
    # # Remove any small characters remaining
    filtered_sentence = [word for word in filtered_sentence if len(word) > 1]
    # # Lemmatise text
    lemmatised_sentence = [wnl.lemmatize(word) for word in filtered_sentence]
    return lemmatised_sentence


def download_sample_text(
        s3_resource,
        bucket='beis-orp-dev-ingest',
        key='txt/00e6929569a9456a8f79e5f1064ef0a3.txt'):

    sample_text = s3_resource.get_object(bucket, key)['Body'].read()
    return sample_text


def download_model(
        s3_resource,
        bucket='beis-orp-dev-clustering-models',
        key='051222_bertopic_longformer_kmeans3'):

    location = f'/tmp/{os.path.basename(key)}'
    if not os.path.exists(location):
        s3_resource.Object(bucket, key).download_file(location)
    return location


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


def classify_data(model_path, input_data):
    model = torch.jit.load(model_path)
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
    uuid = metadata['uuid']

    print(f"Document text: {doc_text}")
    print(f"UUID obtained is: {uuid}")

    # download model
    model_path = download_model(
        s3_resource=s3_resource,
        bucket='beis-orp-dev-clustering-models',
        key='models/pytorch_model.pt'
    )
    # download image
    input_data = download_sample_text(
        s3_resource=s3_resource,
        bucket=event['url']
    )
    # classify text
    topic = classify_data(model_path, input_data)
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
