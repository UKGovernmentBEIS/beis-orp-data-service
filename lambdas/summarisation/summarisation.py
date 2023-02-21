import os
import io
import nltk
import torch
import boto3
import zipfile
import pymongo
from http import HTTPStatus
from ext_sum import summarize
from model_builder import ExtSummarizer
from smart_open import open as smart_open
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()

DDB_USER = os.environ['DDB_USER']
DDB_PASSWORD = os.environ['DDB_PASSWORD']
DDB_DOMAIN = os.environ['DDB_DOMAIN']
SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
MODEL_BUCKET = os.environ['MODEL_BUCKET']
NLTK_DATA_PATH = os.environ['NLTK_DATA']

ddb_connection_uri = f'mongodb://{DDB_USER}:{DDB_PASSWORD}@{DDB_DOMAIN}:27017/?directConnection=true'


def initialisation(resource_path=NLTK_DATA_PATH):
    '''Downloads and unzips alls the resources needed to initialise the model'''

    # Define new directory to tmp directory
    save_path = os.path.join('/tmp', 'mydir')
    os.makedirs(save_path)

    # Define modeldir path
    save_path = os.path.join('/tmp', 'modeldir')
    if not os.path.isdir(save_path):
        os.makedirs(save_path)

    # Create new directories in tmp directory
    os.makedirs(resource_path, exist_ok=True)
    nltk.download('punkt', download_dir=resource_path)

    # Unzip all resources
    with zipfile.ZipFile(os.path.join(resource_path, 'tokenizers', 'punkt.zip'), 'r') as zip_ref:
        zip_ref.extractall(os.path.join(resource_path, 'tokenizers'))

    logger.info('Completed initialisation')

    return None


def download_text(s3_client, document_uid, bucket=SOURCE_BUCKET):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    document = s3_client.get_object(
        Bucket=bucket,
        Key=f'processed/{document_uid}.txt'
    )['Body'].read().decode('utf-8')

    logger.info('Downloaded text')

    return document


def download_model(
        s3_client,
        bucket=MODEL_BUCKET,
        key='mobilebert_ext.pt'):
    '''Downloads the ML model for summarisation'''

    s3_client.download_file(bucket, key, os.path.join("/tmp/modeldir", key))

    # Load the model in
    with smart_open(os.path.join("/tmp/modeldir", key), 'rb') as f:
        CHECKPOINT = io.BytesIO(f.read())
        checkpoint = torch.load(CHECKPOINT, map_location=torch.device("cpu"))
        model = ExtSummarizer(
            checkpoint=checkpoint,
            bert_type="mobilebert",
            device="cpu")

        return model


def smart_shortener(text):
    """
    params: text: Str
    returns: shortened_complete: Str (shortened text to summarise)
    """
    if len(text.split(" ")) < 600:
        return text
    else:
        shortened = " ".join(text.split(" ")[: 600])
        shortened_complete = shortened + text.replace(shortened, "").split(".")[0]
        return shortened_complete


def mongo_connect_and_update(document_uid,
                             summary,
                             database=ddb_connection_uri,
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

    initialisation()

    s3_client = boto3.client('s3')
    document = download_text(s3_client=s3_client, document_uid=document_uid)
    logger.info("Loading model")
    model = download_model(s3_client=s3_client)

    # Shorten text for summarising
    shortened_text = smart_shortener(text=document)
    summary = summarize(
        raw_text_fp=smart_shortener(
            text=shortened_text),
        model=model,
        max_length=4)

    response = mongo_connect_and_update(document_uid=document_uid, summary=summary)
    response['document_uid'] = document_uid

    return response
