import io
import os
# import re
import boto3
# from http import HTTPStatus
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


def download_text(s3_client, object_key, source_bucket):
    '''Downloads the PDF from S3 ready for conversion and metadata extraction'''

    document = s3_client.get_object(
        Bucket=source_bucket,
        Key=object_key
    )['Body'].read()

    doc_bytes_io = io.BytesIO(document)

    logger.info('Downloaded text')

    return doc_bytes_io


def get_s3_metadata(s3_client, object_key, source_bucket):
    '''Gets the S3 metadata attached to the PDF'''

    metadata = s3_client.head_object(
        Bucket=source_bucket,
        Key=object_key
    )['Metadata']

    return metadata


def write_text(s3_client, text, object_key, metadata,
               destination_bucket=DESTINATION_BUCKET):
    '''Write the extracted text to a .txt file in the staging bucket'''

    response = s3_client.put_object(
        Body=text,
        Bucket=destination_bucket,
        Key=f'trigger-pipeline/{object_key}.pdf',
        Metadata=metadata
    )
    logger.info('Saved converted document back to S3')

    return response


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    source_bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    logger.info(
        f'New document in {source_bucket}: {object_key}')

    # Download document and associated metadata
    # Convert to PDF
    # Write PDF and metadata back to bucket
    s3_client = boto3.client('s3')

    doc_s3_metadata = get_s3_metadata(
        s3_client=s3_client, object_key=object_key, source_bucket=source_bucket)
    document_uid = doc_s3_metadata['uuid']
    logger.append_keys(document_uid=document_uid)

    s3_response = write_text(s3_client=s3_client, object_key=object_key,
                             document_uid=document_uid)

    handler_response = {**s3_response}
    handler_response['document_uid'] = document_uid

    return handler_response
