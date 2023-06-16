from io import BytesIO
import os
# import json
import boto3
# from datetime import datetime
from bs4 import BeautifulSoup
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


def download_text(s3_client: boto3.client,
                  document_uid: str,
                  source_bucket=SOURCE_BUCKET) -> BytesIO:
    '''Downloads the ORPML document from S3 ready for conversion and metadata extraction'''

    document = s3_client.get_object(
        Bucket=source_bucket,
        Key=f'processed/{document_uid}.orpml'
    )['Body'].read()

    doc_bytes_io = BytesIO(document)
    logger.info('Downloaded text')

    return doc_bytes_io


def parse_orpml(doc_bytes_io: BytesIO) -> tuple:
    return None


def create_orpml_metadata(orpml_header: dict, enrichments: list) -> dict:
    return None


def create_orpml_body(orpml_body: BeautifulSoup) -> BeautifulSoup:
    return None


def create_orpml_document(orpml_metadata: dict, orpml_body: BeautifulSoup) -> str:
    return None


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


def build_graph_document(orpml_metadata: dict) -> dict:
    return None


@logger.inject_lambda_context(log_event=True)
def handler(event: dict, context: LambdaContext) -> dict:
    logger.set_correlation_id(context.aws_request_id)

    document_uid = event['document_uid']
    enrichments = event['enrichments']

    # Downloading ORPML document from S3
    s3_client = boto3.client('s3')
    doc_bytes_io = download_text(
        s3_client=s3_client,
        document_uid=document_uid,
        source_bucket=SOURCE_BUCKET
    )

    # Pulling the existing metadata and body from the ORPML document
    existing_orpml_metadata, existing_orpml_body = parse_orpml(
        doc_bytes_io=doc_bytes_io)

    # Building a full dictionary of metadata for the ORPML header
    # Joins the current header and the data science enrichments
    final_orpml_metadata = create_orpml_metadata(
        orpml_header=existing_orpml_metadata,
        enrichments=enrichments
    )

    # Cleaning, formatting and prettifying the text body of the ORPML
    final_orpml_body = create_orpml_body(
        orpml_body=existing_orpml_body
    )

    # Joining the final metadata and body to build the final document
    final_orpml_document = create_orpml_document(
        orpml_metadata=final_orpml_metadata,
        orpml_body=final_orpml_body
    )

    # Overwriting the existing ORPML with the finalised ORPML in S3
    write_text(
        s3_client=s3_client,
        text=final_orpml_document,
        document_uid=document_uid,
        destination_bucket=DESTINATION_BUCKET
    )

    # Formats a metadata document to upload to the graph database
    # Maps the ORPML metadata to the graph schema
    metadata_document = build_graph_document(orpml_metadata=final_orpml_metadata)

    return metadata_document
