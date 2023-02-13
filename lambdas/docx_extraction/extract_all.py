# Import modules
import io
import os
import docx
import boto3
import zipfile
import pymongo
from http import HTTPStatus
import xml.etree.ElementTree as ET
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
ddb_connection_uri = os.environ['DOCUMENT_DATABASE']


# Defining elements from openxml schema
WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
PARA = WORD_NAMESPACE + 'p'
TEXT = WORD_NAMESPACE + 't'
TABLE = WORD_NAMESPACE + 'tbl'
ROW = WORD_NAMESPACE + 'tr'
CELL = WORD_NAMESPACE + 'tc'


def download_text(s3_client, document_uid, bucket=SOURCE_BUCKET):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    document = io.BytesIO(s3_client.get_object(
        Bucket=bucket,
        Key=f'raw/{document_uid}.docx'
    )['Body'].read())

    logger.info('Downloaded text')

    return document


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


def mongo_connect_and_push(document_uid,
                           title,
                           text,
                           date_published,
                           database,
                           tlsCAFile='./rds-combined-ca-bundle.pem'):
    '''Connects to the DocumentDB, finds the document matching our UUID and adds the title, text and date published to it'''

    db_client = pymongo.MongoClient(
        database,
        tls=True,
        tlsCAFile=tlsCAFile
    )
    logger.info(db_client.list_database_names())
    db = db_client.bre_orp
    collection = db.documents

    # Insert document to DB
    logger.info({'document': collection.find_one(
        {'document_uid': document_uid})})
    collection.find_one_and_update({'document_uid': document_uid}, {
                                    '$set': {'title': title},
                                    '$set': {'text': text},
                                    '$set': {'date_published': date_published}})
    db_client.close()

    logger.info('Sent to DocumentDB')

    return {'statusCode': HTTPStatus.OK}


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    document_uid = event['document_uid']
    logger.append_keys(document_uid=document_uid)

    s3_client = boto3.client('s3')

    docx_file = download_text(s3_client, document_uid=document_uid)
    doc = docx.Document(docx_file)
    metadata = getMetaData(doc)

    # Get title and date published
    title = metadata["title"]
    date_published = metadata["created"]

    # Get text
    text = get_docx_text(docx_file)

    logger.info(f"All data extracted. E.g. Title extracted: {title}")

    response = mongo_connect_and_push(
        document_uid=document_uid,
        title=title,
        text=text,
        date_published=date_published,
        database=ddb_connection_uri)

    response['document_uid'] = document_uid

    return response
