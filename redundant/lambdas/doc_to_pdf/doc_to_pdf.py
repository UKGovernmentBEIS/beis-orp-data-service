import os
import subprocess
import boto3
import re
from http import HTTPStatus
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']
SOFFICE_PATH = os.environ['SOFFICE_PATH']
os.environ['HOME'] = '/tmp'


def download_doc(s3_client, object_key, source_bucket, file_path):
    '''Downloads the PDF from S3 ready for conversion and metadata extraction'''

    s3_client.download_file(
        Bucket=source_bucket,
        Key=object_key,
        Filename=file_path
    )

    return {'downloadStatusCode': HTTPStatus.OK}


def get_s3_metadata(s3_client, object_key, source_bucket):
    '''Gets the S3 metadata attached to the PDF'''

    metadata = s3_client.head_object(
        Bucket=source_bucket,
        Key=object_key
    )['Metadata']

    return metadata


def convert_word_to_pdf(word_file_path, output_dir, soffice_path=SOFFICE_PATH):
    '''Calls LibreOffice to convert the document to PDF'''

    subprocess.call([soffice_path,
                     '--headless',
                     '--convert-to',
                     'pdf',
                     '--outdir',
                     output_dir,
                     word_file_path])

    return {'conversionStatusCode': HTTPStatus.OK}


def upload_pdf(s3_client, object_key, metadata,
               destination_bucket=DESTINATION_BUCKET):
    '''Write the extracted text to a .txt file in the staging bucket'''

    s3_client.upload_file(
        Filename=f'/tmp/{object_key}',
        Bucket=destination_bucket,
        Key=f'converted-docs/{object_key}',
        ExtraArgs={
            'Metadata': metadata
        }
    )

    return {'uploadStatusCode': HTTPStatus.OK}


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    source_bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    logger.info(
        f'New document in {source_bucket}: {object_key}')

    s3_client = boto3.client('s3')

    raw_key = re.sub('(.*/)*', '', object_key)
    new_key = re.sub('\\.docx?', '.pdf', raw_key)

    download_response = download_doc(
        s3_client=s3_client,
        object_key=object_key,
        source_bucket=source_bucket,
        file_path=f'/tmp/{raw_key}')
    logger.info(download_response)

    doc_s3_metadata = get_s3_metadata(
        s3_client=s3_client,
        object_key=object_key,
        source_bucket=source_bucket)
    logger.info(doc_s3_metadata)

    conversion_response = convert_word_to_pdf(
        word_file_path=f'/tmp/{raw_key}',
        output_dir='/tmp')
    logger.info(conversion_response)

    upload_response = upload_pdf(
        s3_client=s3_client,
        object_key=new_key,
        metadata=doc_s3_metadata)
    logger.info(upload_response)

    handler_response = {**download_response, **
                        conversion_response, **upload_response}

    output = {
        'detail': {
            'object': {
                'key': f'converted-docs/{new_key}'
            },
            'bucket': {
                'name': DESTINATION_BUCKET
            }
        }
    }

    return {**handler_response, **output}
