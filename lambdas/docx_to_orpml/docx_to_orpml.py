import re
import os
import json
import docx
import boto3
import string
import zipfile
from io import BytesIO
from datetime import datetime
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']

# Defining elements from openxml schema
WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
PARA = WORD_NAMESPACE + 'p'
TEXT = WORD_NAMESPACE + 't'
TABLE = WORD_NAMESPACE + 'tbl'
ROW = WORD_NAMESPACE + 'tr'
CELL = WORD_NAMESPACE + 'tc'


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
    '''Downloads the DOCX from S3 ready for conversion and metadata extraction'''

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
    '''Gets the S3 metadata attached to the DOCX'''

    metadata = s3_client.head_object(
        Bucket=source_bucket,
        Key=object_key
    )['Metadata']

    logger.info('Downloaded metadata from S3 object')

    return metadata


def extract_docx_metadata(doc_bytes_io: BytesIO) -> list:
    '''Extracts the metadata in the DOCX'''

    doc = docx.Document(doc_bytes_io)
    prop = doc.core_properties

    if prop.modified:
        date_formatted = datetime.strftime(prop.modified, '%Y-%m-%d')
    elif prop.created:
        date_formatted = datetime.strftime(prop.created, '%Y-%m-%d')
    else:
        date_formatted = None

    docx_meta_tags = {
        'dc:title': prop.title,
        'dc:subject': prop.subject,
        'dc:created': date_formatted,
        'dc:publisher': prop.author,
        'dc:format': 'DOCX',
        'dc:language': prop.language if prop.language else 'en-GB',
        'dc:license': 'OGL',
        'dc:issued': date_formatted,
    }

    logger.info('Extracted metadata from DOCX')

    return docx_meta_tags


def extract_docx_text(doc_bytes_io: BytesIO) -> str:
    '''
    Extracts the entire body of the text in the DOCX
    Other methods only extract certain sections, this extracts the entirety
    '''
    document = zipfile.ZipFile(doc_bytes_io)
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

    whole_text = '\n\n'.join(paragraphs)
    cleaned_text = clean_text(whole_text)

    logger.info('Extracted text from DOCX')

    return cleaned_text


def process_orpml(text_body: str, docx_meta_tags: dict, s3_metadata: dict) -> str:
    '''Builds the ORPML document from the metadata and text extracted from the DOCX'''

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

    meta_tags = {**docx_meta_tags, **s3_meta_tags}

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
    # Create a new HTML tag for the text
    text_tag = orpml.new_tag('div', attrs={'class': 'text'})
    text_tag.string = text_body
    # Append the text tag to the <body>
    body_tag.append(text_tag)

    logger.info('Finished attaching page to ORPML body')

    beautified_orpml = orpml.prettify()
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

    # Extract text and metadata from PDF
    docx_meta_tags = extract_docx_metadata(doc_bytes_io=doc_bytes_io)
    text_body = extract_docx_text(doc_bytes_io=doc_bytes_io)

    # Build ORPML document (insert header and body)
    orpml_doc = process_orpml(
        text_body=text_body,
        docx_meta_tags=docx_meta_tags,
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
