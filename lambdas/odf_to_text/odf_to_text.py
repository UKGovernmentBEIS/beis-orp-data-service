import io
import re
import os
import boto3
import pymongo
import zipfile
import pandas as pd
from http import HTTPStatus
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DDB_USER = os.environ['DDB_USER']
DDB_PASSWORD = os.environ['DDB_PASSWORD']
DDB_DOMAIN = os.environ['DDB_DOMAIN']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']

ddb_connection_uri = f'mongodb://{DDB_USER}:{DDB_PASSWORD}@{DDB_DOMAIN}:27017/?directConnection=true'


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


def convert2xml(odf):
    """
    params: odf
    returns: content, metadata: content xml and metadata xml of the odf
    """
    myfile = zipfile.ZipFile(odf)

    listoffiles = myfile.infolist()

    for s in listoffiles:
        if s.orig_filename == 'content.xml':
            bh = myfile.read(s.orig_filename)
            element = ET.XML(bh)
            ET.indent(element)
            content = ET.tostring(element, encoding='unicode')
        elif s.orig_filename == 'meta.xml':
            bh = myfile.read(s.orig_filename)
            element = ET.XML(bh)
            ET.indent(element)
            metadataXML = ET.tostring(element, encoding='unicode')

    return content, metadataXML


def metadata_title_date_extraction(metadataXML):
    """
    param: metadataXML: metadata of odf file
    returns: modification date of odf
    """
    soup = BeautifulSoup(metadataXML, "lxml")
    metadata = soup.find("ns0:meta")
    title = metadata.find("dc:title").get_text()
    date_published = pd.to_datetime(
        metadata.find("dc:date").get_text()).isoformat()

    return title, date_published


def xml2text(xml):
    """
    params: xml
    returns: text
    """
    soup = BeautifulSoup(xml, "lxml")
    pageText = soup.findAll(text=True)
    text = str(" ".join(pageText)).replace("\n", "")
    return re.sub("\\s+", " ", text)


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
                           database=ddb_connection_uri,
                           tlsCAFile='./rds-combined-ca-bundle.pem'):
    '''Connects to the DocumentDB and inserts extracted metadata from the ODF'''

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
        s3_client=s3_client,
        object_key=object_key,
        source_bucket=source_bucket)
    doc_s3_metadata = get_s3_metadata(
        s3_client=s3_client,
        object_key=object_key,
        source_bucket=source_bucket)

    assert doc_s3_metadata.get('uuid'), 'Document must have a UUID attached'
    document_uid = doc_s3_metadata['uuid']
    logger.append_keys(document_uid=document_uid)

    # Extract the content and metadata xml
    contentXML, metadataXML = convert2xml(odf=doc_bytes_io)
    text = xml2text(xml=contentXML)

    # Extract the publishing date
    title, date_published = metadata_title_date_extraction(metadataXML=metadataXML)

    logger.info(f'Extracted title: {title}'
                f'Publishing date: {date_published}'
                f'UUID obtained is: {document_uid}')

    mongo_response = mongo_connect_and_push(
        source_bucket=source_bucket,
        object_key=object_key,
        document_uid=document_uid,
        date_published=date_published,
        title=title)
    s3_response = write_text(s3_client=s3_client, text=text, document_uid=document_uid)

    handler_response = {**mongo_response, **s3_response}
    handler_response['document_uid'] = document_uid

    return handler_response
