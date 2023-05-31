import io
import os
import json
import boto3
from datetime import datetime
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


def write_text(s3_client, text, document_uid, destination_bucket=DESTINATION_BUCKET):
    '''Write the extracted text to a .txt file in the staging bucket'''

    response = s3_client.put_object(
        Body=text,
        Bucket=destination_bucket,
        Key=f'processed/{document_uid}.txt',
        Metadata={
            'uuid': document_uid
        }
    )
    logger.info('Saved text to data lake')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200, 'Text did not successfully write to S3'

    return None


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    source_bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    logger.info(
        f'New document in {source_bucket}: {object_key}'
    )

    # Finding the time the object was uploaded
    date_uploaded = event['time']
    date_obj = datetime.strptime(date_uploaded, "%Y-%m-%dT%H:%M:%SZ")
    date_uploaded_formatted = date_obj.strftime("%Y-%m-%dT%H:%M:%S")

    s3_client = boto3.client('s3')
    doc_bytes_io = download_text(
        s3_client=s3_client,
        object_key=object_key,
        source_bucket=source_bucket
    )
    doc_s3_metadata = get_s3_metadata(
        s3_client=s3_client,
        object_key=object_key,
        source_bucket=source_bucket
    )

    # Raise an error if there is no UUID in the document's S3 metadata
    assert doc_s3_metadata.get('uuid'), 'Document must have a UUID attached'

    # Getting S3 metadata from S3 object
    document_uid = doc_s3_metadata['uuid']
    regulator_id = doc_s3_metadata.get('regulator_id')
    user_id = doc_s3_metadata.get('user_id')
    api_user = doc_s3_metadata.get('api_user')
    document_type = doc_s3_metadata.get('document_type')
    status = doc_s3_metadata.get('status')
    regulatory_topics = json.loads(doc_s3_metadata.get('topics'))

    write_text(s3_client=s3_client, text=doc_bytes_io,
               document_uid=document_uid, destination_bucket=DESTINATION_BUCKET)

    logger.info('All data extracted')

    # Building metadata document
    doc = {
        # 'title': title,
        'document_uid': document_uid,
        'regulator_id': regulator_id,
        'user_id': user_id,
        'uri': object_key,
        'data':
        {
            'dates':
            {
                # 'date_published': date_published,
                'date_uploaded': date_uploaded_formatted
            }
        },
        'document_type': document_type,
        'document_format': 'ORPML',
        'regulatory_topic': regulatory_topics,
        'status': status,
    }

    handler_response = {
        'document': doc,
        'object_key': object_key,
        'api_user': api_user,
    }

    return handler_response
