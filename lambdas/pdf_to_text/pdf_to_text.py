import io
import os
import re
import boto3
import string
import pandas as pd
import pikepdf
import fitz
from PyPDF2 import PdfReader
import pymongo
from http import HTTPStatus
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

# Testing

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
    docinfo = pdf.docinfo
    dict_docinfo = dict(docinfo.items())

    try:
        title = meta['{http://purl.org/dc/elements/1.1/}title']
    except KeyError:
        title = pdf.docinfo.get('/Title')

    # Get date
    if "/ModDate" in dict_docinfo:
        mod_date = re.search(r"\d{8}", str(docinfo["/ModDate"])).group()
    elif "/CreationDate" in dict_docinfo:
        mod_date = re.search(r"\d{8}", str(docinfo["/CreationDate"])).group()

    date_published = pd.to_datetime(
        mod_date).isoformat()

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

    text = clean_text(text)
    return text


def remove_excess_punctuation(text) -> str:
    """
    param: text: Str document text
    returns: text: Str cleaned document text
        Returns text without excess punctuation
    """
    # Clean punctuation spacing
    text = text.replace(" .", "")
    for punc in string.punctuation:
        text = text.replace(punc + punc, "")
    return text


def clean_text(text):
    '''Clean the text by removing illegal characters and excess whitespace'''
    pattern = re.compile(r'\s+')

    text = text.replace('\n', ' ')
    text = text.replace(' .', '. ')
    text = re.sub('(\\d+(\\.\\d+)?)', r' \1 .', text)
    text = re.sub(pattern, ' ', text)
    text = remove_excess_punctuation(text)
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
    text = re.sub('\\.{4,}', '.', text)

    return text


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

    return handler_response
