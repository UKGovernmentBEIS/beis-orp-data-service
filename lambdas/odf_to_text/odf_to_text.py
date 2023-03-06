import io
import re
import os
import boto3
import zipfile
import pandas as pd
from http import HTTPStatus
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

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


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    logger.info(event)

    source_bucket = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    logger.info(
        f'New document in {source_bucket}: {object_key}'
    )

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

    assert doc_s3_metadata.get('uuid'), 'Document must have a UUID attached'
    document_uid = doc_s3_metadata['uuid']

    # Getting S3 metadata from S3 object
    document_uid = doc_s3_metadata['uuid']
    regulator_id = doc_s3_metadata.get('regulator_id')
    user_id = doc_s3_metadata.get('user_id')
    api_user = doc_s3_metadata.get('api_user')
    document_type = doc_s3_metadata.get('document_type')
    status = doc_s3_metadata.get('status')

    # Extract the content and metadata xml
    contentXML, metadataXML = convert2xml(odf=doc_bytes_io)
    text = xml2text(xml=contentXML)
    s3_response = write_text(
        s3_client=s3_client, text=text, document_uid=document_uid
    )

    # Extract the publishing date
    title, date_published = metadata_title_date_extraction(
        metadataXML=metadataXML
    )

    logger.info(f"All data extracted e.g. Title extracted: {title}")

    # Building metadata document
    doc = {
        "title": title,
        "document_uid": document_uid,
        "regulator_id": regulator_id,
        "user_id": user_id,
        "uri": f's3://{source_bucket}/{object_key}',
        "data":
        {
            "dates":
            {
                "date_published": date_published,
            }
        },
        "document_type": document_type,
        # "regulatory_topic": regulatory_topic,
        "status": status,
    }

    handler_response = {
        'document': doc,
        'object_key': object_key,
        'api_user': api_user,
        **s3_response
    }

    return handler_response
