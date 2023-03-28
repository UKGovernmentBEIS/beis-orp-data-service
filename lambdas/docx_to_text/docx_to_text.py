import io
import os
import boto3
import zipfile
import filetype
import pandas as pd
from tika import parser
from datetime import datetime
import xml.etree.ElementTree as ET
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


def download_text(s3_client, object_key, source_bucket):
    '''Downloads the doc from S3 for data extraction'''

    document = s3_client.get_object(
            Bucket=source_bucket,
            Key=object_key
        )['Body'].read()

    return document


def get_s3_metadata(s3_client, object_key, source_bucket):
    '''Gets the S3 metadata attached to the document'''

    metadata = s3_client.head_object(
        Bucket=source_bucket,
        Key=object_key
    )['Metadata']

    return metadata


def get_docx_text(path):
    '''
    param: path Str: Take the path of a docx file as argument
    returns: paragraphs Str: text in unicode.
        Function from stackoverflow to pull text from a docx file
    '''
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


def get_docx_metadata(docx):
    '''
    param: docx
    returns: metadata
        Function from stackoverflow to get metadata from docx
    '''

    prop = docx.core_properties
    metadata = {
        'author': prop.author,
        'category': prop.category,
        'comments': prop.comments,
        'content_status': prop.content_status,
        'created': prop.created,
        'identifier': prop.identifier,
        'keywords': prop.keywords,
        'last_modified_by': prop.last_modified_by,
        'language': prop.language,
        'modified': prop.modified,
        'subject': prop.subject,
        'title': prop.title,
        'version': prop.version
    }

    return metadata


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


def extract_all_from_doc_file(doc_file):
    """
    params: doc_file: doc file
        returns: text, title, date_published
    """
    parsed = parser.from_file(doc_file)

    text = str(parsed["content"])

    try:
        title = str(parsed["metadata"]["dc:title"])
    except:
        title = ""
    
    date_published = parsed["metadata"]["dcterms:modified"]

    if type(date_published) == list:
        date_published = pd.to_datetime(date_published[0]).isoformat()

    else:
        date_published = pd.to_datetime(date_published).isoformat()

    return text, title, date_published


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    source_bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']

    logger.info(source_bucket, object_key)

    # Finding the time the object was uploaded
    date_uploaded = event['time']
    date_obj = datetime.strptime(date_uploaded, "%Y-%m-%dT%H:%M:%SZ")
    date_uploaded_formatted = date_obj.strftime("%Y-%m-%dT%H:%M:%S")

    s3_client = boto3.client('s3')
    doc_s3_metadata = get_s3_metadata(
        s3_client=s3_client,
        object_key=object_key,
        source_bucket=source_bucket
    )

    # Raise an error if there is no UUID in the document's S3 metadata
    assert doc_s3_metadata.get('uuid'), 'Document must have a UUID attached'

    # Getting S3 metadata from S3 object
    document_uid = doc_s3_metadata['uuid']
    regulator_id = doc_s3_metadata.get('regulator_id')
    user_id = doc_s3_metadata.get('user_id')
    api_user = doc_s3_metadata.get('api_user')
    document_type = doc_s3_metadata.get('document_type')
    status = doc_s3_metadata.get('status')

    docx_file = download_text(
        s3_client=s3_client,
        object_key=object_key,
        source_bucket=source_bucket
    )
    doc_bytes_io = io.BytesIO(docx_file)
    logger.info(type(doc_bytes_io))
    # If file type is .doc
    if filetype.guess(doc_bytes_io).extension == "doc":
        logger.info("File is a doc file")
        logger.info(docx_file)
        logger.info(type(docx_file))
        # Text, title, date_published
        text, title, date_published = extract_all_from_doc_file(docx_file)

    # Else file type is .docx
    else:
        logger.info("File is a docx file")
        docx = docx.Document(doc_bytes_io)
        metadata = get_docx_metadata(docx=docx)
        # Text, title, date_published
        text = get_docx_text(path=docx_file)
        title = metadata['title']
        date_published = pd.to_datetime(metadata['created']).isoformat()

    # Push text to s3 bucket
    write_text(s3_client=s3_client, text=text, document_uid=document_uid)

    logger.info(f'All data extracted e.g. Title extracted: {title}')

    # Building metadata document
    doc = {
        'title': title,
        'document_uid': document_uid,
        'regulator_id': regulator_id,
        'user_id': user_id,
        'uri': object_key,
        'data':
        {
            'dates':
            {
                'date_published': date_published,
                'date_uploaded': date_uploaded_formatted
            }
        },
        'document_type': document_type,
        'document_format': 'DOCX',
        'status': status,
    }

    handler_response = {
        'document': doc,
        'object_key': object_key,
        'api_user': api_user,
    }

    return handler_response