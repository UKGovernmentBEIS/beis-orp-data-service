import io
import os
import docx
import boto3
import zipfile
import pymongo
import pandas as pd
from http import HTTPStatus
import xml.etree.ElementTree as ET
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DDB_USER = os.environ['DDB_USER']
DDB_PASSWORD = os.environ['DDB_PASSWORD']
DDB_DOMAIN = os.environ['DDB_DOMAIN']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']

ddb_connection_uri = f'mongodb://{DDB_USER}:{DDB_PASSWORD}@{DDB_DOMAIN}:27017/?directConnection=true'

# Defining elements from openxml schema
WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
PARA = WORD_NAMESPACE + 'p'
TEXT = WORD_NAMESPACE + 't'
TABLE = WORD_NAMESPACE + 'tbl'
ROW = WORD_NAMESPACE + 'tr'
CELL = WORD_NAMESPACE + 'tc'


def download_text(s3_client, object_key, source_bucket):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    document = io.BytesIO(s3_client.get_object(
        Bucket=source_bucket,
        Key=object_key,
    )['Body'].read())

    logger.info('Downloaded text')

    return document


def get_s3_metadata(s3_client, object_key, source_bucket):
    '''Gets the S3 metadata attached to the document'''

    metadata = s3_client.head_object(
        Bucket=source_bucket,
        Key=object_key
    )['Metadata']

    return metadata


def get_docx_text(path):
    """
    param: path Str: Take the path of a docx file as argument
    returns: paragraphs Str: text in unicode.
        Function from stackoverflow to pull text from a docx file
    """
    document = zipfile.ZipFile(path)
    xml_content = document.read('word/document.xml')
    document.close()
    tree = ET.XML(xml_content)

    paragraphs = []
    for paragraph in tree.iter(PARA):
        texts = [node.text
                 for node in paragraph.iter(TEXT)
                 if node.text]
        if texts:
            paragraphs.append(''.join(texts))

    return '\n\n'.join(paragraphs)


def getMetaData(doc):
    """
    param: doc docx
    returns: metadata
        Function from stackoverflow to get metadata from docx
    """

    prop = doc.core_properties
    metadata = {
        "author": prop.author,
        "category": prop.category,
        "comments": prop.comments,
        "content_status": prop.content_status,
        "created": prop.created,
        "identifier": prop.identifier,
        "keywords": prop.keywords,
        "last_modified_by": prop.last_modified_by,
        "language": prop.language,
        "modified": prop.modified,
        "subject": prop.subject,
        "title": prop.title,
        "version": prop.version
    }

    return metadata


def mongo_connect_and_push(source_bucket,
                           object_key,
                           document_uid,
                           title,
                           date_published,
                           database,
                           tlsCAFile='./rds-combined-ca-bundle.pem'):
    '''Connects to the DocumentDB and inserts extracted metadata from the DOC'''

    db_client = pymongo.MongoClient(
        database,
        tls=True,
        tlsCAFile=tlsCAFile
    )
    logger.info(db_client.list_database_names())
    db = db_client.bre_orp
    collection = db.documents

    doc = {
        'title': title,
        'date_published': date_published,
        'document_uid': document_uid,
        'uri': f's3://{source_bucket}/{object_key}',
        'object_key': object_key
    }

    # Insert document to DB if it doesn't already exist
    if not collection.find_one(doc):
        collection.insert_one(doc)
    logger.info(f'Document inserted: {collection.find_one(doc)}')

    db_client.close()
    return {**doc, 'mongoStatusCode': HTTPStatus.OK}


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

    return response


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    source_bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']

    s3_client = boto3.client('s3')
    doc_s3_metadata = get_s3_metadata(
        s3_client=s3_client, object_key=object_key, source_bucket=source_bucket)

    # Raise an error if there is no UUID in the document's S3 metadata
    assert doc_s3_metadata.get('uuid'), 'Document must have a UUID attached'
    document_uid = doc_s3_metadata['uuid']
    logger.append_keys(document_uid=document_uid)

    docx_file = download_text(
        s3_client=s3_client, object_key=object_key, source_bucket=source_bucket)

    doc = docx.Document(docx_file)
    metadata = getMetaData(doc=doc)

    # Get title and date published
    title = metadata["title"]
    date_published = pd.to_datetime(
        metadata["created"]).isoformat()

    # Get and push text to destination bucket
    text = get_docx_text(path=docx_file)
    write_text(s3_client=s3_client, text=text, document_uid=document_uid)

    logger.info(f"All data extracted. E.g. Title extracted: {title}")

    mongo_response = mongo_connect_and_push(
        source_bucket=source_bucket,
        object_key=object_key,
        document_uid=document_uid,
        title=title,
        date_published=date_published,
        database=ddb_connection_uri)

    s3_response = write_text(
        s3_client=s3_client,
        text=text,
        document_uid=document_uid,
        destination_bucket=DESTINATION_BUCKET)

    handler_response = {**mongo_response, **s3_response}
    return handler_response
