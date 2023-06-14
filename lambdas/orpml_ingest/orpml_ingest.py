from io import BytesIO
import os
import json
import boto3
from datetime import datetime
from bs4 import BeautifulSoup
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


def download_text(s3_client: boto3.client,
                  object_key: str,
                  source_bucket: str) -> BytesIO:
    '''Downloads the ORPML document from S3 ready for conversion and metadata extraction'''

    document = s3_client.get_object(
        Bucket=source_bucket,
        Key=object_key
    )['Body'].read()

    doc_bytes_io = BytesIO(document)

    logger.info('Downloaded text')

    return doc_bytes_io


def get_s3_metadata(s3_client: boto3.client,
                    object_key: str,
                    source_bucket: str) -> dict:
    '''Gets the S3 metadata attached to the PDF'''

    metadata = s3_client.head_object(
        Bucket=source_bucket,
        Key=object_key
    )['Metadata']

    return metadata


def process_orpml(doc_bytes_io: BytesIO, metadata: dict) -> str:
    '''Attaches key metadata to the ORPML header'''

    # Reading in the S3 document and parsing it as HTML
    orpml_doc = doc_bytes_io.read()
    orpml = BeautifulSoup(orpml_doc, features='xml')

    # Finding the time the object was uploaded
    date_uploaded = datetime.now()
    date_uploaded_formatted = date_uploaded.strftime("%Y-%m-%dT%H:%M:%S")

    # Turning the S3 metadata into HTML meta tags
    regulatory_topics = json.loads(metadata.get('topics'))
    regulatory_topics_formatted = ', '.join(regulatory_topics)

    meta_tags = {
        # The commented out lines should already be defined in the ingested ORPML
        # 'dc:title': 'PLACEHOLDER',
        # 'dc:subject': 'PLACEHOLDER',
        # 'dc:created': 'PLACEHOLDER',
        # 'dc:publisher': 'PLACEHOLDER',
        'dc:format': 'ORPML',
        'dc:language': 'en-GB',
        'dc:license': 'OGL',
        # 'dc:issued': 'PLACEHOLDER',
        'dc:identifier': metadata['uuid'],
        'orp:regulatorId': metadata['regulator_id'],
        'dc:contributor': metadata['regulator_id'],
        'orp:userId': metadata['user_id'],
        'dc:type': metadata['document_type'],
        'orp:status': metadata['status'],
        'orp:regulatoryTopic': regulatory_topics_formatted,
        'orp:dateUploaded': date_uploaded_formatted,
        'orp:uri': metadata['uri'],
    }

    # Attaching the meta tags to the ORPML header
    dublinCore = orpml.metadata.dublinCore
    dcat = orpml.metadata.dcat
    orp_meta = orpml.metadata.orp
    for k, v in meta_tags.items():
        new_meta = orpml.new_tag(k.split(':')[1])
        new_meta.string = v if v else ''
        if k.startswith('dc:'):
            dublinCore.append(new_meta)
        elif k.startswith('dcat:'):
            dcat.append(new_meta)
        elif k.startswith('orp:'):
            orp_meta.append(new_meta)

    logger.info('Finished attaching metadata to ORPML header')

    beautified_orpml = orpml.prettify()
    return str(beautified_orpml)


def write_text(s3_client: boto3.client,
               text: str,
               document_uid: str,
               destination_bucket=DESTINATION_BUCKET) -> None:
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
def handler(event: dict, context: LambdaContext) -> dict:
    logger.set_correlation_id(context.aws_request_id)

    # Finding the object key of the newly uploaded document
    source_bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    logger.info(
        f'New document in {source_bucket}: {object_key}'
    )

    # Downloading document and S3 metadata from S3
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

    # Inserting the S3 metadata into the ORPML header
    orpml_document = process_orpml(doc_bytes_io=doc_bytes_io, metadata=doc_s3_metadata)

    # Getting crucial S3 metadata from S3 object
    document_uid = doc_s3_metadata['uuid']
    user_id = doc_s3_metadata.get('user_id')
    api_user = doc_s3_metadata.get('api_user')

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
