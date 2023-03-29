import os
import io
import boto3
import torch
from langdetect import detect
from smart_open import open
from utils import smart_postprocessor, smart_shortener
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()

def validate_env_variable(env_var_name):
    logger.debug(
        f"Getting the value of the environment variable: {env_var_name}")
    try:
        env_variable = os.environ[env_var_name]
    except KeyError:
        raise Exception(f"Please, set environment variable {env_var_name}")
    if not env_variable:
        raise Exception(f"Please, provide environment variable {env_var_name}")
    return env_variable


def download_model(
        s3_client,
        bucket,
        key='summ.pt'):
    '''Downloads the ML model for summarisation'''

    s3_client.download_file(bucket, key, os.path.join('/tmp/modeldir', key))

    # Load the model in
    with open(os.path.join('/tmp/modeldir', key), 'rb') as f:
        model = io.BytesIO(f.read())
        summarizer = torch.load(model, map_location=torch.device('cpu'))
        return summarizer


def initialisation():
     # Define modeldir path
    model_path = os.path.join('/tmp', 'modeldir')
    os.makedirs(model_path, exist_ok=True)
    return 


def download_text(s3_client, document_uid, bucket):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    document = s3_client.get_object(
        Bucket=bucket,
        Key=f'processed/{document_uid}.txt'
    )['Body'].read().decode('utf-8')
    logger.info('Downloaded text')

    return document


def detect_language(text):
    """
    Detect language
    param: text: Str
        returns: Str: language of the document
    """
    language = detect(smart_shortener(text))
    return language


def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    SOURCE_BUCKET = validate_env_variable('SOURCE_BUCKET')
    MODEL_BUCKET = validate_env_variable('MODEL_BUCKET')

    document_uid = event['document']['document_uid']

    initialisation()
    s3_client = boto3.client('s3')

    summarizer= download_model(s3_client=s3_client, bucket=MODEL_BUCKET)

    text = download_text(s3_client=s3_client, document_uid=document_uid, bucket=SOURCE_BUCKET)

    # Detect language
    lang = detect_language(text=text)

    # Shorten text after summarising
    summary = smart_postprocessor(
                    summarizer(text))

    logger.info(f'Langauge: {lang}')
    logger.info(f'Summary: {summary}')

    handler_response = event
    handler_response['lambda'] = 'summarisation'

    handler_response['document']['language'] = lang
    handler_response['document']['summary'] = summary

    return handler_response