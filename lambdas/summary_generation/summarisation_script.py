import os
import nltk
import torch
import boto3
import zipfile
import pymongo
from http import HTTPStatus
from ext_sum import summarize
from model_builder import ExtSummarizer
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DOCUMENT_DATABASE = os.environ['DOCUMENT_DATABASE']
SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
NLTK_DATA_PATH = os.environ['NLTK_DATA']


def initialisation(resource_path=NLTK_DATA_PATH):
    '''Downloads and unzips alls the resources needed to initialise the model'''

    # Create new directories in tmp directory
    os.makedirs(resource_path, exist_ok=True)
    nltk.download('punkt', download_dir=resource_path)

    # Unzip all resources
    with zipfile.ZipFile(os.path.join(resource_path, 'tokenizers', 'punkt.zip'), 'r') as zip_ref:
        zip_ref.extractall(os.path.join(resource_path, 'tokenizers'))

    logger.info('Completed initialisation')

    return None


def download_text(s3_client, document_uid, Bucket=SOURCE_BUCKET):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    document = s3_client.get_object(
        Bucket=SOURCE_BUCKET,
        Key=f'processed/{document_uid}.txt'
    )['Body'].read().decode('utf-8')

    logger.info('Downloaded text')

    return document


def load_model(model_type):
    # Load model checkpoint
    checkpoint = torch.load(f"checkpoints/{model_type}_ext.pt", map_location="cpu")
    model = ExtSummarizer(checkpoint=checkpoint, bert_type=model_type, device="cpu")
    return model


def smart_shortener(text):
    if len(text.split(" ")) < 600:
        return text
    else:
        shortened = " ".join(text.split(" ")[ : 600])
        shortened_complete = shortened + text.replace(shortened, "").split(".")[0]
        return shortened_complete


def clean_summary(summary):
    summary_list = summary.strip().split(" ")
    enum_summary = enumerate(summary_list)
    for idx, word in enum_summary:
        if word.isupper() == False and enum_summary[idx + 1].isupper() == True:
            summary_list.insert(idx + 1, ".")
    return " ".join(summary_list)


def mongo_connect_and_update(document_uid,
                             summary,
                             database=DOCUMENT_DATABASE,
                             tlsCAFile='./rds-combined-ca-bundle.pem'):
    '''Connects to the DocumentDB, finds the document matching our UUID and adds the summary to it'''

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
                                   '$set': {'summary': summary}})
    db_client.close()

    logger.info('Sent to DocumentDB')

    return {'statusCode': HTTPStatus.OK}


def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)
    document_uid = event['document_uid']
    logger.append_keys(document_uid=document_uid)

    s3_client = boto3.client('s3')
    document = download_text(s3_client, document_uid)

    initialisation()

    logger.info("Loading model")
    model = load_model("mobilebert")
    shortend_text = smart_shortener(document)
    summary = summarize(smart_shortener(shortend_text), model, max_length=4)
    cleaned_summary = clean_summary(summary)

    response = mongo_connect_and_update(document, cleaned_summary)
    response['document_uid'] = document_uid

    return response