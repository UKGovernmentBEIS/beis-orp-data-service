import os
import io
import re
import nltk
import torch
import boto3
import zipfile
from ext_sum import summarize
from model_builder import ExtSummarizer
from smart_open import open as smart_open
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()

SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
MODEL_BUCKET = os.environ['MODEL_BUCKET']
NLTK_DATA_PATH = os.environ['NLTK_DATA']


def initialisation(resource_path=NLTK_DATA_PATH):
    '''Downloads and unzips alls the resources needed to initialise the model'''

    # Define new directory to tmp directory
    save_path = os.path.join('/tmp', 'mydir')
    os.makedirs(save_path, exist_ok=True)

    # Define modeldir path
    model_path = os.path.join('/tmp', 'modeldir')
    os.makedirs(model_path, exist_ok=True)

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

    s3_client.download_file(bucket, key, os.path.join('/tmp/modeldir', key))

    # Load the model in
    with smart_open(os.path.join('/tmp/modeldir', key), 'rb') as f:
        CHECKPOINT = io.BytesIO(f.read())
        checkpoint = torch.load(CHECKPOINT, map_location=torch.device('cpu'))
        model = ExtSummarizer(
            checkpoint=checkpoint,
            bert_type='mobilebert',
            device='cpu')

        return model


def smart_shortener(text):
    '''
    params: text: Str
    returns: shortened_complete: Str (shortened text to summarise)
    '''
    if len(text.split(' ')) < 600:
        return text
    else:
        shortened = ' '.join(text.split(' ')[: 600])
        shortened_complete = shortened + \
            text.replace(shortened, '').split('.')[0]
        return shortened_complete


def smart_postprocessor(sentence):
    if len(sentence.split(' ')) < 100:
        return sentence
    else:
        shortened = ' '.join(sentence.split(' ')[: 100])
        end_sentence = sentence.replace(shortened, '')
        shortened_complete = shortened + end_sentence.split('.')[0] + '.'
        if len(shortened_complete) > 1000:
            res = [match.start()
                   for match in re.finditer(r'[A-Z]', end_sentence)]
            shortened_complete = shortened + end_sentence[:res[0] - 1] + '.'
            return shortened_complete
        else:
            return shortened_complete


def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    document_uid = event['document']['document_uid']

    initialisation()

    s3_client = boto3.client('s3')
    document = download_text(s3_client=s3_client, document_uid=document_uid)
    logger.info('Loading model')
    model = download_model(s3_client=s3_client)

    # Shorten text for summarising
    shortened_text = smart_shortener(text=document)
    summary = smart_postprocessor(
        summarize(
            shortened_text,
            model,
            max_length=4
        )
    )
    logger.info(f'Summary: {summary}')

    handler_response = event
    handler_response['lambda'] = 'summarisation'

    handler_response['document']['summary'] = summary

    return handler_response
