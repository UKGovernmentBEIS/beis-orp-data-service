import os
import re
import nltk
import boto3
import pikepdf
import pymongo
import numpy as np   
import pandas as pd 
from logging import Logger
from http import HTTPStatus
from nltk.corpus import stopwords
from preprocess.preprocess_functions import preprocess
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM 
from postprocess.postprocess_functions import postprocess_title
from aws_lambda_powertools.utilities.typing import LambdaContext
from search_metadata_title.get_title import identify_metadata_title_in_text

logger = Logger()

DOCUMENT_DATABASE = os.environ['DOCUMENT_DATABASE']
SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
MODEL_BUCKET = os.environ['MODEL_BUCKET']
NLTK_DATA_PATH = os.environ['NLTK_DATA']
MODEL_PATH = os.environ['MODEL_PATH']

my_pattern = re.compile(r'\s+')


# Extract title from metadata of document
def extract_title(doc_bytes_io):
    '''Extracts title from PDF streaming input'''

    pdf = pikepdf.Pdf.open(doc_bytes_io)
    meta = pdf.open_metadata()
    try:
        title = meta['{http://purl.org/dc/elements/1.1/}title']
    except KeyError:
        title = pdf.docinfo.get('/Title')

    return title


# # Heuristic-based function to decide on approach to title extraction
# def use_automatic_title_extraction(title):
#     """
#     params: title: metadata title extracted from the PDF
#     """
#     # Remove stopwords
#     title = " ".join([word for word in title.strip().split(" ") if word not in stopwords.words("english")])
#     # Remove punctuation
#     title = re.sub(r"[^\w\s]", "", title).strip()
#     # Remove Microsoft Word from titles
#     title = re.sub("Microsoft Word", "", title)
#     # Remove excess white space
#     title = re.sub(my_pattern, " ", title)
#     # Heuristic: if the number of tokens in the title is less than 4 or greater than 35
#     # Then use automatic title extraction
#     title_length = len(title.split(" "))
#     if (title_length > 35) or (title_length < 4):
#         return True
#     else:
#         return False


# Define predictor function
def title_predictor(text, tokenizer, model):

    tokenizer = AutoTokenizer.from_pretrained("fabiochiu/t5-small-medium-title-generation")
    model = AutoModelForSeq2SeqLM.from_pretrained("fabiochiu/t5-small-medium-title-generation")

    # Preprocess the text
    text = preprocess(text)
    inputs = ["summarize: " + text]
    inputs = tokenizer(inputs, truncation=True, return_tensors="pt")
    output = model.generate(**inputs, num_beams=10, do_sample=False, min_length=10, max_new_tokens=25)
    decoded_output = tokenizer.batch_decode(output, skip_special_tokens=True)[0]
    predicted_title = nltk.sent_tokenize(decoded_output.strip())[0]

    # Postprocess the text
    processed_title = postprocess_title(predicted_title)
    return processed_title


def get_title(title, text, threshold):

    junk = ["Microsoft Word - ", ".Doc", ".doc"]

    # Remove junk
    for j in junk:
        title = re.sub(j, "", str(title))

    # Remove excess whitespace
    title = re.sub(my_pattern, " ", title)

    # Immediately filter out long metadata titles
    if (len(title.split(" ")) > 40):
        title = title_predictor(text)
        return title

    else:
        score = identify_metadata_title_in_text(title, text)

        # If score is greater than 95% and title is longer than 2 tokens
        if score >= 95 and (len(title.split(" ")) > 2):
            title = title_predictor(text)
            return title

        elif (score > threshold) and (len(title.split(" ")) > 3):
            return title
            
        else:
            title = title_predictor(text)
            return title


def download_text(s3_client, document_uid, bucket=SOURCE_BUCKET):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    document = s3_client.get_object(
        Bucket=bucket,
        Key=f'processed/{document_uid}.txt'
    )['Body'].read().decode('utf-8')

    logger.info('Downloaded text')

    return document


def mongo_connect_and_push(document_uid,
                           title,
                           database=DOCUMENT_DATABASE,
                           tlsCAFile='./rds-combined-ca-bundle.pem'):
    '''Connects to the DocumentDB, finds the document matching our UUID and adds the title to it'''

    db_client = pymongo.MongoClient(
        database,
        tls=True,
        tlsCAFile=tlsCAFile
    )
    logger.info(db_client.list_database_names())
    db = db_client.bre_orp
    collection = db.documents

    # Insert document to DB
    logger.info({'document': collection.find_one(
        {'document_uid': document_uid})})
    collection.find_one_and_update({'document_uid': document_uid}, {
                                   '$set': {'title': title}})
    db_client.close()

    logger.info('Sent to DocumentDB')

    return {'statusCode': HTTPStatus.OK}


def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    s3_client = boto3.client('s3')

    document_uid = event['document_uid']
    metadata_title = extract_title()

    text = download_text(s3_client, document_uid)
    title = get_title(metadata_title, text, 85)

    response = mongo_connect_and_push(document_uid, title)
    response['document_uid'] = document_uid

    return response
