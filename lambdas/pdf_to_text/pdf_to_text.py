import io
import os
import re
import boto3
import pikepdf
import fitz
import pymongo
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DOCUMENT_DATABASE = os.environ['DOCUMENT_DATABASE']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


def download_text(s3_client, object_key, source_bucket):
    '''Downloads the PDF from S3 ready for conversion and metadata extraction'''

    document = s3_client.get_object(
        Bucket=source_bucket,
        Key=object_key
    )['Body'].read().decode('utf-8')

    doc_bytes = document.read()
    doc_bytes_io = io.BytesIO(doc_bytes)

    logger.info('Downloaded text')

    return doc_bytes_io


def get_s3_metadata(s3_client, object_key, source_bucket):
    '''Gets the S3 metadata attached to the PDF'''

    metadata = s3_client.head_object(
        Bucket=source_bucket,
        Key=object_key
    )['Metadata']

    return metadata


def extract_title(doc_bytes_io):
    '''Extracts title from PDF streaming input'''

    pdf = pikepdf.Pdf.open(doc_bytes_io)
    meta = pdf.open_metadata()
    try:
        title = meta['{http://purl.org/dc/elements/1.1/}title']
    except KeyError:
        title = pdf.docinfo.get('/Title')

    return title


def extract_text(doc_bytes_io):
    '''Extracts text from PDF streaming input'''

    with fitz.open(stream=doc_bytes_io) as doc:
        text = ''
        for page in doc:
            text += page.get_text()

    return text


def clean_text(text):
    '''Clean the text by removing illegal characters and excess whitespace'''

    text = re.sub('\n', ' ', text)
    text = re.sub(ILLEGAL_CHARACTERS_RE, ' ', text)

    # Space out merged words by adding a space before a capital letter
    # if it appears after a lowercase letter
    text = re.sub(
        r'([a-z](?=[A-Z])|[A-Z](?=[A-Z][a-z]))',
        r'\1 ',
        text
    )

    text = text.strip()
    text = text.lower()
    text = text.replace('\t', ' ')
    text = text.replace('_x000c_', '')
    text = re.sub('\\s+', ' ', text)
    text = re.sub('<.*?>', '', text)
    text = re.sub('\.{4,}', '.')

    return text


def cut_title(title):
    '''Cuts title length down to 25 tokens'''

    title = re.sub('Figure 1', '', title)
    title = re.sub(r'[^\w\s]', '', title)

    if len(str(title).split(' ')) > 25:
        title = ' '.join(title.split(' ')[0:25]) + '...'

    return title


def mongo_connect_and_push(source_bucket,
                           object_key,
                           document_uid,
                           title,
                           database=DOCUMENT_DATABASE,
                           tlsCAFile='./rds-combined-ca-bundle.pem'):
    '''Connects to the DocumentDB and inserts extracted metadata from the PDF'''

    # Create a MongoDB client and open a connection to Amazon DocumentDB
    db_client = pymongo.MongoClient(
        database,
        tls=True,
        tlsCAFile='./rds-combined-ca-bundle.pem'
    )
    logger.info('Connected to DocumentDB')

    db = db_client.bre_orp
    collection = db.documents

    doc = {
        'title': title,
        'document_uid': document_uid,
        'uri': f's3://{source_bucket}/{object_key}',
        'object_key': object_key
    }

    # Insert document to DB if it doesn't already exist
    if not collection.find_one(doc):
        collection.insert_one(doc)
        logger.info('Inserted document to DocumentDB')
    logger.info(f'Document inserted: {collection.find_one(doc)}')

    db_client.close()
    return None


def write_text(s3_client, text, document_uid, destination_bucket=DESTINATION_BUCKET):
    '''Write the extracted text to a .txt file in the staging bucket'''

    response = s3_client.put_object(
        Body=text,
        Bucket=DESTINATION_BUCKET,
        Key=f'processed/{document_uid}.txt',
        Metadata={
            'uuid': document_uid
        }
    )
    logger.info('Saved text to data lake')

    return response


@logger.inject_lambda_context(log_event=True)
def handler(event, context):
    logger.set_correlation_id(context.aws_request_id)

    print(f'Event received: {event}')
    source_bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    logger.info(
        f'New document in {source_bucket}: {object_key}')

    s3_client = boto3.client('s3')
    doc_bytes_io = download_text(
        s3_client=s3_client, object_key=object_key, source_bucket=source_bucket)
    doc_s3_metadata = get_s3_metadata(
        s3_client=s3_client, object_key=object_key, source_bucket=source_bucket)
    document_uid = doc_s3_metadata['uuid']
    logger.append_keys(document_uid=document_uid)

    title = extract_title(doc_bytes_io)
    text = extract_text(doc_bytes_io)
    logger.info(f'Extracted title: {title}'
                'UUID obtained is: {document_uid}')

    mongo_connect_and_push(source_bucket=source_bucket,
                           object_key=object_key, document_uid=document_uid, title=title)

    response = write_text(s3_client=s3_client, text=text,
                          document_uid=document_uid)
    logger.info(response)

    return {
        'statusCode': 200,
        'document_uid': document_uid
    }
