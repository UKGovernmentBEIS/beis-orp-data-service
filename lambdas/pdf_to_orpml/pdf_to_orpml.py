import os
import re
import json
import pdfplumber
import boto3
import string
from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


def remove_excess_punctuation(text: str) -> str:
    '''Removes excess punctuation (obvs lol)'''

    text = text.replace(' .', '')
    for punc in string.punctuation:
        text = text.replace(punc + punc, '')
    return text


def clean_text(text: str) -> str:
    '''Clean the text by removing illegal characters and excess whitespace'''

    pattern = re.compile(r'\s+')

    text = str(text).replace('\n', ' ')
    text = text.replace(' .', '. ')
    text = re.sub('(\\d+(\\.\\d+)?)', r' \1 .', text)
    text = re.sub(pattern, ' ', text)
    text = remove_excess_punctuation(text=text)
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
    text = text.encode('ascii', 'ignore').decode('utf-8')
    text = re.sub('\\s+', ' ', text)
    text = re.sub('<.*?>', '', text)
    text = re.sub('\\.{4,}', '.', text)

    return text


def download_text(s3_client: boto3.client,
                  object_key: str,
                  source_bucket: str) -> BytesIO:
    '''Downloads the PDF from S3 ready for conversion and metadata extraction'''

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

    logger.info('Downloaded metadata from S3 object')

    return metadata


def extract_pdf_metadata(doc_bytes_io: BytesIO) -> list:
    '''Extracts the metadata in the PDF'''

    with pdfplumber.open(doc_bytes_io) as pdf:
        metadata = pdf.metadata

    if metadata.get('ModDate'):
        date = datetime.strptime(metadata.get('ModDate')[2:-7], '%Y%m%d%H%M%S')
        date_formatted = datetime.strftime(date, '%Y-%m-%d')
    elif metadata.get('CreationDate'):
        date = datetime.strptime(metadata.get(
            'CreationDate')[2:-7], '%Y%m%d%H%M%S')
        date_formatted = datetime.strftime(date, '%Y-%m-%d')
    else:
        date_formatted = None

    pdf_meta_tags = {
        'dc:title': metadata.get('Title'),
        'dc:subject': metadata.get('Subject'),
        'dc:created': date_formatted,
        'dc:publisher': metadata.get('Author'),
        'dc:format': 'PDF',
        'dc:language': 'en-GB',
        'dc:license': 'OGL',
        'dc:issued': date_formatted,
    }

    logger.info('Extracted metadata from PDF')

    return pdf_meta_tags


def extract_pdf_text(doc_bytes_io: BytesIO) -> list:
    '''Extracts the body of the text in the PDF'''

    pages = list()
    with pdfplumber.open(doc_bytes_io) as pdf:
        for i, page in enumerate(pdf.pages):
            # Extract text content from the page
            page_content = page.extract_text().strip()

            # Remove excess punctuation from the text
            page_content = clean_text(text=page_content)
            pages.append(page_content)

    logger.info('Extracted text from PDF')

    return pages


def process_orpml(pages: dict, pdf_meta_tags: dict, s3_metadata: dict) -> str:
    '''Builds the ORPML document from the metadata and text extracted from the PDF'''

    orpml = BeautifulSoup(
        '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
            <orpml xmlns="http://www.beis.gov.uk/namespaces/orpml">
              <metadata>
                <dublinCore>
                </dublinCore>
                <dcat>
                </dcat>
                <orp>
                </orp>
              </metadata>
              <documentContent>
                <html>
                  <body>
                  </body>
                </html>
              </documentContent>
            </orpml>''',
        features='xml'
    )

    # Finding the time the object was uploaded
    date_uploaded = datetime.now()
    date_uploaded_formatted = date_uploaded.strftime('%Y-%m-%dT%H:%M:%S')

    # Turning the S3 metadata into HTML meta tags
    regulatory_topics = json.loads(s3_metadata.get('topics'))
    regulatory_topics_formatted = ', '.join(regulatory_topics)

    s3_meta_tags = {
        'dc:identifier': s3_metadata['uuid'],
        'orp:regulatorId': s3_metadata['regulator_id'],
        'dc:contributor': s3_metadata['regulator_id'],
        'orp:userId': s3_metadata['user_id'],
        'dc:type': s3_metadata['document_type'],
        'orp:status': s3_metadata['status'],
        'orp:regulatoryTopic': regulatory_topics_formatted,
        'orp:dateUploaded': date_uploaded_formatted,
        'orp:uri': s3_metadata['uri'],
    }

    meta_tags = {**pdf_meta_tags, **s3_meta_tags}

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

    body_tag = orpml.find('body')
    # Iterate over the pages and append them to the <body> tag
    for page in pages:
        # Create a new HTML tag for the page
        page_tag = orpml.new_tag('div', attrs={'class': 'page'})
        page_tag.string = page

        # Append the page tag to the HTML
        body_tag.append(page_tag)

    logger.info('Finished attaching page to ORPML body')

    return str(orpml)


def write_text(s3_client: boto3.client,
               text: str,
               document_uid: str,
               destination_bucket=DESTINATION_BUCKET) -> None:
    '''Write the extracted text to a .orpml file in the data lake'''

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

    # Extract text and metadata from PDF
    pdf_meta_tags = extract_pdf_metadata(doc_bytes_io=doc_bytes_io)
    text_pages = extract_pdf_text(doc_bytes_io=doc_bytes_io)

    # Build ORPML document (insert header and body)
    orpml_doc = process_orpml(
        pages=text_pages,
        pdf_meta_tags=pdf_meta_tags,
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
