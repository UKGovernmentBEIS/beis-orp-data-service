import io
import os
import re
import boto3
import pikepdf
import fitz
from PyPDF2 import PdfReader
import pymongo
from http import HTTPStatus
from datetime import datetime
from time import mktime, strptime
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DDB_USER = os.environ['DDB_USER']
DDB_PASSWORD = os.environ['DDB_PASSWORD']
DDB_DOMAIN = os.environ['DDB_DOMAIN']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']

ddb_connection_uri = f'mongodb://{DDB_USER}:{DDB_PASSWORD}@{DDB_DOMAIN}:27017/?directConnection=true'


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


def extract_title_and_date(doc_bytes_io):
    '''Extracts title from PDF streaming input'''

    pdf = pikepdf.Pdf.open(doc_bytes_io)
    meta = pdf.open_metadata()
    try:
        title = meta['{http://purl.org/dc/elements/1.1/}title']
    except KeyError:
        title = pdf.docinfo.get('/Title')
    date_string = re.sub(
        r'[a-zA-Z]', r' ', meta['{http://ns.adobe.com/xap/1.0/}ModifyDate']).strip()[0:19]
    date_published = datetime.fromtimestamp(
        mktime(strptime(date_string, "%Y-%m-%d %H:%M:%S")))

    return str(title), date_published


def extract_text(doc_bytes_io):
    '''Extracts text from PDF streaming input'''

    try:
        # creating a pdf reader object
        reader = PdfReader(doc_bytes_io)

        # printing number of pages in pdf file
        # print(len(reader.pages))

        totalPages = PdfReader.numPages

        # getting a specific page from the pdf file
        text = []
        for page in range(0, totalPages):
            page = reader.pages[page]
            # extracting text from page
            txt = page.extract_text()
            text.append(txt)

        text = " ".join(text)

    except BaseException:
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
    text = text.replace('\t', ' ')
    text = text.replace('_x000c_', '')
    text = re.sub('\\s+', ' ', text)
    text = re.sub('<.*?>', '', text)
    text = re.sub('\\.{4,}', '.')

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
                           date_published,
                           database,
                           tlsCAFile='./rds-combined-ca-bundle.pem'):
    '''Connects to the DocumentDB and inserts extracted metadata from the PDF'''

    # Create a MongoDB client and open a connection to Amazon DocumentDB
    db_client = pymongo.MongoClient(
        database,
        tls=True,
        tlsCAFile=tlsCAFile
    )

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
    return {'mongoStatusCode': HTTPStatus.OK}


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
    logger.info(
        f'New document in {source_bucket}: {object_key}')

    s3_client = boto3.client('s3')
    doc_bytes_io = download_text(
        s3_client=s3_client, object_key=object_key, source_bucket=source_bucket)
    doc_s3_metadata = get_s3_metadata(
        s3_client=s3_client, object_key=object_key, source_bucket=source_bucket)

    # Raise an error if there is no UUID in the document's S3 metadata
    assert doc_s3_metadata.get('uuid'), 'Document must have a UUID attached'
    document_uid = doc_s3_metadata['uuid']

    logger.append_keys(document_uid=document_uid)

    title, date_published = extract_title_and_date(doc_bytes_io=doc_bytes_io)
    text = extract_text(doc_bytes_io=doc_bytes_io)
    logger.info(f'Extracted title: {title}'
                f'UUID obtained is: {document_uid}'
                f'Date published is: {date_published}')

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
    handler_response['document_uid'] = document_uid

    return handler_response
