import os
import re
import json
import pdfplumber
import boto3
import string
from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup
from bs4.formatter import HTMLFormatter
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


class CustomHTMLFormatter(HTMLFormatter):
    def attributes(self, tag):
        for k, v in tag.attrs.items():
            yield k, v


def remove_excess_punctuation(text) -> str:
    '''
    param: text: Str document text
        Returns text without excess punctuation
    '''
    # Clean punctuation spacing
    text = text.replace(' .', '')
    for punc in string.punctuation:
        text = text.replace(punc + punc, '')
    return text


def clean_text(text):
    '''Clean the text by removing illegal characters and excess whitespace'''
    pattern = re.compile(r'\s+')

    text = str(text).replace('\n', ' ')
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
    text = text.encode('ascii', 'ignore').decode("utf-8")
    text = re.sub('\\s+', ' ', text)
    text = re.sub('<.*?>', '', text)
    text = re.sub('\\.{4,}', '.', text)

    return text


def download_text(s3_client, object_key, source_bucket):
    '''Downloads the PDF from S3 ready for conversion and metadata extraction'''

    document = s3_client.get_object(
        Bucket=source_bucket,
        Key=object_key
    )['Body'].read()

    doc_bytes_io = BytesIO(document)

    logger.info('Downloaded text')

    return doc_bytes_io


def get_s3_metadata(s3_client, object_key, source_bucket):
    '''Gets the S3 metadata attached to the PDF'''

    metadata = s3_client.head_object(
        Bucket=source_bucket,
        Key=object_key
    )['Metadata']

    return metadata


def extract_pdf_metadata(doc_bytes_io: BytesIO) -> dict:
    with pdfplumber.open(doc_bytes_io) as pdf:
        metadata = pdf.metadata

    if metadata.get('ModDate'):
        date = datetime.strptime(metadata.get('ModDate')[2:-7], "%Y%m%d%H%M%S")
    elif metadata.get('CreationDate'):
        date = datetime.strptime(metadata.get('CreationDate')[2:-7], "%Y%m%d%H%M%S")
    date_formatted = datetime.strftime(date, "%Y-%m-%d")

    pdf_metadata = {
        "DC.title": metadata.get('Title'),
        "DC.subject": metadata.get('Subject'),
        "DC.date": date_formatted,
        "DC.publisher": metadata.get('Author'),
    }

    return pdf_metadata


def extract_pdf_text(doc_bytes_io: BytesIO) -> dict:
    pages = dict()
    with pdfplumber.open(doc_bytes_io) as pdf:
        for i, page in enumerate(pdf.pages):
            # Extract text content from the page
            page_content = page.extract_text().strip()

            # Remove excess punctuation from the text
            page_content = clean_text(page_content)
            pages[f'page_{i+1}'] = page_content

    return pages


def process_orpml(pages: dict, pdf_metadata: dict, s3_metadata: dict) -> str:
    return 'hello'


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

    # Finding the object key of the newly uploaded document
    source_bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    logger.info(
        f'New document in {source_bucket}: {object_key}'
    )

    # Finding the time the object was uploaded
    date_uploaded = datetime.strptime(event['time'], "%Y-%m-%dT%H:%M:%SZ")
    date_uploaded_formatted = datetime.strftime(date_uploaded, "%Y-%m-%dT%H:%M:%S")

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

    # Extract text and metadata from PDF
    pdf_metadata = extract_pdf_metadata(doc_bytes_io=doc_bytes_io)
    pdf_metadata['DC.dateSubmitted'] = date_uploaded_formatted
    text_pages = extract_pdf_text(doc_bytes_io=doc_bytes_io)

    # Build ORPML document (insert header and body)
    orpml_doc = process_orpml(
        pages=text_pages,
        pdf_metadata=pdf_metadata,
        s3_metadata=doc_s3_metadata
    )

    # Getting crucial S3 metadata from S3 object
    document_uid = doc_s3_metadata['uuid']
    user_id = doc_s3_metadata.get('user_id')
    api_user = doc_s3_metadata.get('api_user')

    # Write ORPML to S3
    write_text(
        s3_client=s3_client,
        text=orpml_doc,
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
