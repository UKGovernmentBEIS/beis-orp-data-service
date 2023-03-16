import os
import io
import boto3
import torch
from smart_open import open as smart_open
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
MODEL_BUCKET = os.environ['MODEL_BUCKET']
MODEL_PATH = os.environ['MODEL_PATH']


def download_text(s3_client, document_uid, bucket=SOURCE_BUCKET):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    document = s3_client.get_object(
        Bucket=bucket,
        Key=f'processed/{document_uid}.txt'
    )['Body'].read().decode('utf-8')
    logger.info('Downloaded text')

    return document


def download_model(s3_client,
                   bucket=MODEL_BUCKET,
                   model_path=MODEL_PATH,
                   key='keybert.pt'):
    '''Downloads the ML model for keyword extraction'''

    s3_client.download_file(
        bucket,
        key,
        os.path.join(model_path, key)
    )
    with smart_open(os.path.join(model_path, key), 'rb') as f:
        buffer = io.BytesIO(f.read())
        model = torch.load(buffer)
    logger.info('Downloaded model')

    return model


def extract_legislative_origin(document):
    # Insert DynamoDB query here
    return None


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    document_uid = event['document']['document_uid']

    logger.info('Started initialisation...')
    os.makedirs(MODEL_PATH, exist_ok=True)

    s3_client = boto3.client('s3')
    document = download_text(s3_client=s3_client, document_uid=document_uid)
    # model = download_model(s3_client=s3_client)

    item = extract_legislative_origin(document=document)

    legislative_origins = [
        {
            "url": item["href"],
            "ref": item["ref"],
            "title": item["title"],
            "number": item["number"],
            "type": item["legType"],
            "division": item["legDivision"]
        }
    ]

    handler_response = event
    handler_response['lambda'] = 'legislative_origin_extraction'
    handler_response['document']['legislative_origins'] = legislative_origins

    return handler_response
