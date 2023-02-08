import os
import re
import nltk
import boto3
import pikepdf
import pymongo
import numpy as np   
import pandas as pd 
from logging import Logger
from preprocess.preprocess_functions import preprocess
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM 
from postprocess.postprocess_functions import postprocess_title
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DOCUMENT_DATABASE = os.environ['DOCUMENT_DATABASE']
SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
MODEL_BUCKET = os.environ['MODEL_BUCKET']
NLTK_DATA_PATH = os.environ['NLTK_DATA']
MODEL_PATH = os.environ['MODEL_PATH']


# Import pre-trained title extraction model

tokenizer = AutoTokenizer.from_pretrained("fabiochiu/t5-small-medium-title-generation")
model = AutoModelForSeq2SeqLM.from_pretrained("fabiochiu/t5-small-medium-title-generation")

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

# Heuristic-based function to decide on approach to title extraction
def use_automatic_title_extraction(title):
    """
    params: title: metadata title extracted from the PDF
    """
    # Remove punctuation
    title = re.sub(r"[^\w\s]", "", title).strip()
    # Remove Microsoft Word from titles
    title = re.sub("Microsoft Word", "", title)
    # Remove excess white space
    title = re.sub(my_pattern, " ", title)
    # Heuristic: if the number of tokens in the title is less than 4
    # Then use automatic title extraction
    if 35 < len(title.split(" ")) < 4:
        return True
    else:
        return False

# Define predictor function
def title_predictor(text):
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

def download_text(s3_client, document_uid, bucket=SOURCE_BUCKET):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    document = s3_client.get_object(
        Bucket=bucket,
        Key=f'processed/{document_uid}.txt'
    )['Body'].read().decode('utf-8')

    logger.info('Downloaded text')

    return document

def handler(event, context: LambdaContext):
    s3_client = boto3.client('s3')
    document_uid = event['document_uid']
    metadata_title = extract_title()
    if use_automatic_title_extraction(title):
        text = download_text(s3_client, document_uid)
        title = title_predictor(text)
        return title
    else:
        title = re.sub("Microsoft Word - ", "", metadata_title)
        title = re.sub(my_pattern, " ", title)
        return title