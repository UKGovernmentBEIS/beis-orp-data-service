import io
import os
import json
import boto3
from datetime import datetime
from bs4 import BeautifulSoup
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


def download_text(s3_client, object_key, source_bucket):
    '''Downloads the ORPML document from S3 ready for conversion and metadata extraction'''

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


def process_orpml(doc_bytes_io, metadata):
    '''Attaches key metadata to the ORPML header'''

    # with open(doc_bytes_io, 'r') as fp:
    orpml_doc = doc_bytes_io.read()

    soup = BeautifulSoup(orpml_doc, 'html.parser')

    # Finding the time the object was uploaded
    date_uploaded = datetime.now()
    date_uploaded_formatted = date_uploaded.strftime("%Y-%m-%dT%H:%M:%S")

    regulatory_topics = json.loads(metadata.get('topics'))
    regulatory_topics_formatted = ', '.join(regulatory_topics)

    meta_tags = [
        {'name': 'DC.identifier', 'content': metadata['uuid']},
        {'name': 'DC.regulatorId', 'content': metadata['regulator_id']},
        {'name': 'DC.userId', 'content': metadata['user_id']},
        {'name': 'DC.type', 'content': metadata['document_type']},
        {'name': 'DC.status', 'content': metadata['status']},
        {'name': 'DC.regulatoryTopic', 'content': regulatory_topics_formatted},
        {'name': 'DC.dateSubmitted', 'content': date_uploaded_formatted},
        {'name': 'DC.uri', 'content': metadata['uri']},
    ]

    head = soup.head
    for meta_tag in meta_tags:
        new_meta = soup.new_tag("meta", attrs=meta_tag)
        head.append(new_meta)

    logger.info('Finished attaching metadata to ORPML header')
    return str(soup)


def write_text(s3_client, text, document_uid, destination_bucket=DESTINATION_BUCKET):
    '''Write the processed ORPML to a .orpml file in the data lake'''

    response = s3_client.put_object(
        Body=text,
        Bucket=destination_bucket,
        Key=f'processed/{document_uid}.orpml',
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
    doc_s3_metadata['uri'] = object_key

    # Raise an error if there is no UUID in the document's S3 metadata
    assert doc_s3_metadata.get('uuid'), 'Document must have a UUID attached'
    api_user = doc_s3_metadata.get('api_user')

    # Inserting the metadata into the ORPML header
    orpml_document = process_orpml(doc_bytes_io=doc_bytes_io, metadata=doc_s3_metadata)

    # Getting crucial S3 metadata from S3 object
    document_uid = doc_s3_metadata['uuid']
    user_id = doc_s3_metadata.get('user_id')

    # Writing the processed ORPML to S3
    write_text(
        s3_client=s3_client,
        text=orpml_document,
        document_uid=document_uid,
        destination_bucket=DESTINATION_BUCKET
    )

    # Passing key metadata onto the next function
    return {
        'document_uid': document_uid,
        'user_id': user_id,
        'api_user': api_user,
        'uri': object_key
    }
