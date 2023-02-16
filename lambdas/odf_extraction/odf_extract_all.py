import io
import os
import boto3
import pymongo
import datefinder
from bs4 import BeautifulSoup
import re
import xml.etree.ElementTree as ET
import zipfile
from odf import text, teletype
from http import HTTPStatus
from odf.opendocument import load
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
DOCUMENT_DATABASE = os.environ['DOCUMENT_DATABASE']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


def download_text(s3_client, object_key, source_bucket):
    '''Downloads the ODF from S3 ready for conversion and metadata extraction'''

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


def return_elements(doc_bytes_io):
    """
    params: doc_bytes_io: doc bytes from s3 bucket
    returns: elements: odf.element.Element object
    """
    odf = load(doc_bytes_io)
    logger.info(odf)
    elements = odf.getElementsByType(text.P)
    return elements


def title_extraction(elements):
    """
    params: elements: odf.element.Element
    returns: title Str: the title of the document where the attribute value
        is equal to "Title"
    """
    titles = []
    for element in elements:
        el_attributes = element.attributes
        if "title" in str(list(el_attributes.values())[0]).lower() and "sub" not in str(list(el_attributes.values())[0]).lower():
            title = teletype.extractText(element)
            titles.append(title)
    if len(titles) > 0:
        return titles[0]
    else:
        return ""


def publishing_date_extraction(elements):
    """
    params: elements: odf.element.Element
    returns: match Str: date found in the footer of the document
    """
    for element in elements:
        el_attributes = element.attributes
        if list(el_attributes.values())[0] == "Footer":
            text = teletype.extractText(element)
            matches = [date for date in datefinder.find_dates(text, strict=True)] # Set strict to True to collect well formed dates
            # If length of matches is greater than 1, return the last strict date format found
            if len(matches) > 1:
                return matches[-1]
            # Else return the date found
            else:
                for match in matches:
                    return str(match)


def date_extraction(text):
    """
    params: elements: odf.element.Element
    returns: match Str: date found in the footer of the document
    """
    matches = datefinder.find_dates(text)
    date_matches = [str(date) for date in matches]
    return date_matches[0]


def convert2xml(odf):
    """
    params: odf
    returns: xml
    """
    myfile = zipfile.ZipFile(odf)

    listoffiles = myfile.infolist()

    for s in listoffiles:
        if s.orig_filename == 'content.xml':
                bh = myfile.read(s.orig_filename)
                element = ET.XML(bh)
                ET.indent(element)

    return ET.tostring(element, encoding='unicode')


def xml2text(xml):
    """
    params: xml
    returns: text
    """
    soup = BeautifulSoup(xml, "lxml")   
    pageText = soup.findAll(text=True)
    text = str(" ".join(pageText)).replace("\n", "")
    return re.sub("\s+", " ", text)


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


def mongo_connect_and_push(source_bucket,
                           object_key,
                           document_uid,
                           date_published,
                           title,
                           database=DOCUMENT_DATABASE,
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
        'document_uid': document_uid,
        'date_published': date_published,
        'uri': f's3://{source_bucket}/{object_key}',
        'object_key': object_key
    }

    # Insert document to DB if it doesn't already exist
    if not collection.find_one(doc):
        collection.insert_one(doc)
    logger.info(f'Document inserted: {collection.find_one(doc)}')

    db_client.close()
    return {'mongoStatusCode': HTTPStatus.OK}


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    logger.info(event)

    source_bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    logger.info(
        f'New document in {source_bucket}: {object_key}')

    s3_client = boto3.client('s3')
    doc_bytes_io = download_text(
        s3_client=s3_client, object_key=object_key, source_bucket=SOURCE_BUCKET)
    doc_s3_metadata = get_s3_metadata(
        s3_client=s3_client, object_key=object_key, source_bucket=SOURCE_BUCKET)
    document_uid = doc_s3_metadata['uuid']
    logger.append_keys(document_uid=document_uid)

    # Load the ODF and extract elements
    elements = return_elements(doc_bytes_io)

    # Extract the title
    title = title_extraction(elements)

    # Extract the publishing date
    date_published = publishing_date_extraction(elements)

    # Extract the text
    xml = convert2xml(doc_bytes_io)
    text = xml2text(xml)

    logger.info(f'Extracted title: {title}'
                f'Publishing date: {date_published}'
                f'UUID obtained is: {document_uid}')

    mongo_response = mongo_connect_and_push(source_bucket=SOURCE_BUCKET,
                                            object_key=object_key, document_uid=document_uid, 
                                            date_published = date_published, title=title)
    s3_response = write_text(s3_client=s3_client, text=text,
                             document_uid=document_uid)

    handler_response = {**mongo_response, **s3_response}
    handler_response['document_uid'] = document_uid

    return handler_response