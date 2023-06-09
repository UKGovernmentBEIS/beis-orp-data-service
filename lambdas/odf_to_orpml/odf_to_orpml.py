import os
import re
import json
import boto3
import string
import zipfile
from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup
from bs4.formatter import HTMLFormatter
import xml.etree.ElementTree as ET
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


class CustomHTMLFormatter(HTMLFormatter):
    def attributes(self, tag):
        for k, v in tag.attrs.items():
            yield k, v


def remove_excess_punctuation(text: str) -> str:
    '''Removes excess punctuation'''

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
    '''Downloads the ODF from S3 ready for conversion and metadata extraction'''

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
    '''Gets the S3 metadata attached to the ODF'''

    metadata = s3_client.head_object(
        Bucket=source_bucket,
        Key=object_key
    )['Metadata']

    logger.info('Downloaded metadata from S3 object')

    return metadata


def extract_meta_and_content(doc_bytes_io: BytesIO) -> tuple:
    '''Converts the content and metadata to XML for easier parsing'''
    zip = zipfile.ZipFile(doc_bytes_io)

    listoffiles = zip.infolist()

    for s in listoffiles:
        if s.orig_filename == 'content.xml':
            bh = zip.read(s.orig_filename)
            element = ET.XML(bh)
            ET.indent(element)
            contentXML = ET.tostring(element, encoding='unicode')
        elif s.orig_filename == 'meta.xml':
            bh = zip.read(s.orig_filename)
            element = ET.XML(bh)
            ET.indent(element)
            metadataXML = ET.tostring(element, encoding='unicode')

    return contentXML, metadataXML


def extract_odf_metadata(metadataXML: str) -> list:
    '''Extracts the metadata in the ODF'''

    soup = BeautifulSoup(metadataXML, 'lxml')
    metadata = soup.find('ns0:meta')

    date = datetime.strptime(metadata.find('dc:date'), '%Y-%m-%dT%H:%M:%SZ')
    date_formatted = datetime.strftime(date, '%Y-%m-%d')

    odf_meta_tags = [
        {'name': 'DC.title', 'content': metadata.find('dc:title').text},
        {'name': 'DC.subject', 'content': metadata.find('dc:subject').text},
        {'name': 'DC.date', 'content': date_formatted},
        {'name': 'DC.publisher', 'content': metadata.find('creator').text},
        {'name': 'DC.format', 'content': 'ODF'}
    ]

    logger.info('Extracted metadata from ODF')

    return odf_meta_tags


def extract_odf_text(contentXML: str) -> str:
    '''Extracts the body of the text in the ODF'''

    soup = BeautifulSoup(contentXML, 'lxml')
    pageText = soup.findAll(text=True)

    text = str(' '.join(pageText)).replace('\n', '')
    logger.info('Extracted text from ODF')

    return clean_text(text=text)


def process_orpml(text_body: str, odf_meta_tags: dict, s3_metadata: dict) -> str:
    '''Builds the ORPML document from the metadata and text extracted from the ODF'''

    orpml = BeautifulSoup(
        '<!DOCTYPE orpml><orpml><head></head><body></body></orpml>',
        'html.parser'
    )

    # Finding the time the object was uploaded
    date_uploaded = datetime.now()
    date_uploaded_formatted = date_uploaded.strftime('%Y-%m-%dT%H:%M:%S')

    # Turning the S3 metadata into HTML meta tags
    regulatory_topics = json.loads(s3_metadata.get('topics'))
    regulatory_topics_formatted = ', '.join(regulatory_topics)

    s3_meta_tags = [
        {'name': 'DC.identifier', 'content': s3_metadata['uuid']},
        {'name': 'DTAC.regulatorId', 'content': s3_metadata['regulator_id']},
        {'name': 'DTAC.userId', 'content': s3_metadata['user_id']},
        {'name': 'DC.type', 'content': s3_metadata['document_type']},
        {'name': 'DTAC.status', 'content': s3_metadata['status']},
        {'name': 'DTAC.regulatoryTopic', 'content': regulatory_topics_formatted},
        {'name': 'DTAC.dateSubmitted', 'content': date_uploaded_formatted},
        {'name': 'DTAC.uri', 'content': s3_metadata['uri']},
    ]

    meta_tags = odf_meta_tags + s3_meta_tags

    # Attaching the meta tags to the ORPML header
    head = orpml.head
    for meta_tag in meta_tags:
        new_meta = orpml.new_tag("meta", attrs=meta_tag)
        head.append(new_meta)

    logger.info('Finished attaching metadata to ORPML header')

    body_tag = orpml.find('body')
    # Create a new HTML tag for the text
    text_tag = orpml.new_tag('div', attrs={'class': 'text'})
    text_tag.string = text_body
    # Append the text tag to the <body>
    body_tag.append(text_tag)

    logger.info('Finished attaching page to ORPML body')

    beautified_orpml = orpml.prettify(formatter=CustomHTMLFormatter())
    return str(beautified_orpml)


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

    # Extract text and metadata from ODF
    contentXML, metadataXML = extract_meta_and_content(
        doc_bytes_io=doc_bytes_io)
    odf_meta_tags = extract_odf_metadata(metadataXML=metadataXML)
    text_body = extract_odf_text(contentXML=contentXML)

    # Build ORPML document (insert header and body)
    orpml_doc = process_orpml(
        text_body=text_body,
        odf_meta_tags=odf_meta_tags,
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
