from io import BytesIO
import os
import re
import json
import textwrap
import boto3
from bs4 import BeautifulSoup
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


def download_text(s3_client: boto3.client,
                  document_uid: str,
                  source_bucket=SOURCE_BUCKET) -> BytesIO:
    '''Downloads the ORPML document from S3 ready for conversion and metadata extraction'''

    document = s3_client.get_object(
        Bucket=source_bucket,
        Key=f'processed/{document_uid}.orpml'
    )['Body'].read()

    doc_bytes_io = BytesIO(document)
    logger.info('Downloaded text')

    return doc_bytes_io


def parse_beautifulsoup_element(metadata_tag: str, orpml: BeautifulSoup) -> dict:
    '''Takes a parent metadata tag and returns the child elements as a dictionary'''

    metadata_elem = orpml.find(metadata_tag)
    output_dict = {}
    for child in metadata_elem.find_all(recursive=False):
        tag = child.name
        value = child.text if child.text else ''
        output_dict[tag] = value

    return output_dict


def parse_orpml(doc_bytes_io: BytesIO) -> tuple:
    '''
    Parses the existing ORPML document and returns a dictionary of the 
    metadata header and the content in the body
    '''

    # Reading in the S3 document and parsing it as HTML
    orpml_doc = doc_bytes_io.read()
    orpml = BeautifulSoup(orpml_doc, features='xml')

    orpml_body = orpml.find('body')
    orpml_header = {}

    orpml_header['dublinCore'] = parse_beautifulsoup_element(
        metadata_tag='dublinCore', orpml=orpml)
    orpml_header['dcat'] = parse_beautifulsoup_element(
        metadata_tag='dcat', orpml=orpml)
    orpml_header['orp'] = parse_beautifulsoup_element(
        metadata_tag='orp', orpml=orpml)

    logger.info('Parsed the existing ORPML')
    # logger.info(orpml_body)

    return orpml_header, orpml_body


def create_orpml_metadata(orpml_header: dict, enrichments: list) -> dict:
    '''
    Takes the existing header and further enrichments and wraps them up into a new
    dictionary ready to be transformed into the ORPML header
    '''

    merged_enrichments = {}
    for e in enrichments:
        merged_enrichments.update(e)

    orpml_header['dublinCore']['created'] = merged_enrichments.get(
        'date_published')
    orpml_header['dublinCore']['title'] = merged_enrichments.get('title')
    orpml_header['dublinCore']['language'] = merged_enrichments.get('lang')
    orpml_header['dcat']['keywords'] = merged_enrichments.get('keywords')
    orpml_header['dcat']['relatedResource'] = merged_enrichments.get(
        'legislative_origins')
    orpml_header['orp']['summary'] = merged_enrichments.get('summary')

    logger.info('Created new ORPML header')

    return orpml_header


def create_orpml_document(orpml_metadata: dict, orpml_body: str) -> str:
    '''Creates the final ORPML document from the newly processed metadata header and body'''

    # logger.info(orpml_body)

    final_orpml = BeautifulSoup(
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
                </html>
              </documentContent>
            </orpml>''',
        features='xml'
    )

    dublincore_element = final_orpml.find("dublinCore")
    dcat_element = final_orpml.find("dcat")
    orp_element = final_orpml.find("orp")

    # Update the dublinCore element with the values from the metadata
    for key, value in orpml_metadata["dublinCore"].items():
        element = final_orpml.new_tag(key)
        element.string = value
        dublincore_element.append(element)

    # Update the dcat element with the keywords from the metadata
    keywords_element = final_orpml.new_tag("keywords")
    dcat_element.append(keywords_element)
    for keyword in orpml_metadata["dcat"]["keywords"]:
        keyword_element = final_orpml.new_tag("keyword")
        keyword_element.string = keyword
        keywords_element.append(keyword_element)

    # Update the dcat element with the relatedResource from the metadata
    related_resource_element = final_orpml.new_tag("relatedResource")
    dcat_element.append(related_resource_element)
    if orpml_metadata["dcat"]["relatedResource"]:
        for resource in orpml_metadata["dcat"]["relatedResource"]:
            resource_element = final_orpml.new_tag("legislativeOrigin")
            for attr, attr_value in resource.items():
                if attr != "title":
                    resource_attr_element = final_orpml.new_tag(attr)
                    resource_attr_element.string = attr_value
                    resource_element.append(resource_attr_element)
            resource_title_element = final_orpml.new_tag("resourceTitle")
            resource_title_element.string = resource["title"]
            resource_element.append(resource_title_element)
            related_resource_element.append(resource_element)

    # Update the orp element with the values from the metadata
    for key, value in orpml_metadata["orp"].items():
        element = final_orpml.new_tag(key)
        element.string = value
        orp_element.append(element)

    # Prettify and wrap the ORPML body
    prettified_body = orpml_body.prettify()
    final_orpml_body = textwrap.fill(prettified_body, width=80)

    # Write the prettified ORPML body
    body_element = final_orpml.find("html")
    body_element.string = final_orpml_body

    prettified_orpml = final_orpml.prettify()
    final_orpml_document = re.sub(r'\n\s*\n', '\n', prettified_orpml)

    logger.info('Created the final ORPML document')

    return final_orpml_document


def write_text(s3_client: boto3.client,
               text: str,
               document_uid: str,
               destination_bucket=DESTINATION_BUCKET) -> None:
    '''Write the processed ORPML to a .orpml file in the data lake'''

    response = s3_client.put_object(
        Body=text,
        Bucket=destination_bucket,
        Key=f'processed/{document_uid}.orpml',
        Metadata={
            'uuid': document_uid
        }
    )
    logger.info('Overwritten existing ORPML in data lake')
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200, 'Text did not successfully write to S3'

    return None


def build_graph_document(orpml_metadata: dict, event: dict) -> dict:
    '''
    Takes the ORPML metadata dict and maps it to the metadata schema the graph
    is expecting
    '''

    metadata_document = {
        "document_uid": event['document_uid'],
        "regulator_id": orpml_metadata['orp']['regulatorId'],
        "user_id": orpml_metadata['orp']['userId'],
        "uri": orpml_metadata['orp']['uri'],
        "document_type": event['document_type'],
        "document_format": orpml_metadata['dublinCore']['format'],
        "regulatory_topic": event['regulatory_topic'],
        "status": event['status'],
        "hash_text": event['hash_text'],
        "data": {
            "dates": {
                "date_published": event['date_created'],
                "date_uploaded": orpml_metadata['orp']['dateUploaded']
            },
            "legislative_origins": orpml_metadata['dcat'].get('relatedResource')
        },
        "subject_keywords": orpml_metadata['dcat']['keywords'],
        "title": orpml_metadata['dublinCore']['title'],
        "summary": orpml_metadata['orp']['summary'],
        "language": orpml_metadata['dublinCore']['language']
    }

    logger.info('Finished building metadata document for graph')

    return metadata_document


@logger.inject_lambda_context(log_event=True)
def handler(event: dict, context: LambdaContext) -> dict:
    logger.set_correlation_id(context.aws_request_id)

    document_uid = event['document_uid']
    enrichments = event['enrichments']

    # Downloading ORPML document from S3
    s3_client = boto3.client('s3')
    doc_bytes_io = download_text(
        s3_client=s3_client,
        document_uid=document_uid,
        source_bucket=SOURCE_BUCKET
    )

    # Pulling the existing metadata and body from the ORPML document
    existing_orpml_metadata, existing_orpml_body = parse_orpml(
        doc_bytes_io=doc_bytes_io)

    # Building a full dictionary of metadata for the ORPML header
    # Joins the current header and the data science enrichments
    final_orpml_metadata = create_orpml_metadata(
        orpml_header=existing_orpml_metadata,
        enrichments=enrichments
    )

    # Joining the final metadata and body to build the final document
    final_orpml_document = create_orpml_document(
        orpml_metadata=final_orpml_metadata,
        orpml_body=existing_orpml_body
    )

    # Overwriting the existing ORPML with the finalised ORPML in S3
    write_text(
        s3_client=s3_client,
        text=final_orpml_document,
        document_uid=document_uid,
        destination_bucket=DESTINATION_BUCKET
    )

    # Formats a metadata document to upload to the graph database
    # Maps the ORPML metadata to the graph schema
    metadata_document = build_graph_document(
        orpml_metadata=final_orpml_metadata,
        event=event)

    return metadata_document
