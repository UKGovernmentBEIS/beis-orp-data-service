import re
import os
import json
from datetime import datetime
import boto3
import requests
import pandas as pd
from bs4 import BeautifulSoup
from htmldate import find_date
from govuk_extraction import get_content
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


def get_title_and_text(URL: str) -> tuple:
    '''
    params: request URL
        returns: title, text: str
        returns: None: if bad URL is uploaded
    '''
    try:
        req = requests.get(URL)
        soup = BeautifulSoup(req.text, 'html.parser')

        title = str(soup.head.title.get_text())
        text = re.sub(
            "\\s+", " ", str(soup.body.find(id="contentContainer").get_text()).replace("\n", " "))
        return title, text

    except AttributeError:
        try:
            req = requests.get(URL)
            soup = BeautifulSoup(req.text, 'html.parser')
            ol = soup.find("ol")
            if ol is not None:
                title = re.sub(
                    "\\s+", " ", str([i.text for i in ol.findAll("li")][-1]).replace("\n", " ").strip())
            else:
                title = str(soup.head.title.get_text())
            text = re.sub(
                "\\s+", " ", str(" ".join([i.text for i in soup.main.findAll("p")])).replace("\n", " "))
            return title, text

        except AttributeError:
            try:
                req = requests.get(URL)
                soup = BeautifulSoup(req.text, 'html.parser')

                ol = soup.find("ol")
                if ol is not None:
                    title = re.sub(
                        "\\s+", " ", str([i.text for i in ol.findAll("li")][-1]).replace("\n", " ").strip())
                else:
                    title = str(soup.head.title.get_text())
                container = soup.body.find(id="mainContent")
                text = re.sub(
                    "\\s+", " ", str(" ".join([i.text for i in container.findAll("p")])).replace("\n", " "))
                print("Option 2")
                return title, text

            except BaseException:
                return None


def get_publication_modification_date(URL: str) -> str:
    '''Extracts publication date from HTML'''

    # Initally disable extensive search
    publication_date = str(
        find_date(URL, original_date=True, extensive_search=False))
    modification_date = str(find_date(URL, extensive_search=False))

    # If no concrete date is found, do extensive search
    if (publication_date == 'None') and (modification_date == 'None'):
        publication_date = str(find_date(URL, original_date=True))

    publication_date = pd.to_datetime(publication_date).isoformat()

    return publication_date


def process_orpml(text_body: str, metadata: dict) -> str:
    '''Builds the ORPML document from the metadata and text extracted from the URL'''

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

    # Turning the metadata into HTML meta tags
    regulatory_topics = metadata.get('topics')
    regulatory_topics_formatted = ', '.join(regulatory_topics)

    meta_tags = {
        'dc:identifier': metadata['uuid'],
        'orp:regulatorId': metadata['regulator_id'],
        'dc:contributor': metadata['regulator_id'],
        'orp:userId': metadata['user_id'],
        'dc:type': metadata['document_type'],
        'orp:status': metadata['status'],
        'orp:regulatoryTopic': regulatory_topics_formatted,
        'orp:dateUploaded': date_uploaded_formatted,
        'orp:uri': metadata['uri'],
        'dc:title': metadata.get('title'),
        # 'dc:subject': metadata.get('subject'),
        'dc:created': metadata.get('date_published'),
        'dc:publisher': metadata.get('regulator_id'),
        'dc:format': 'HTML',
        'dc:language': 'en-GB',
        'dc:license': 'OGL',
        'dc:issued': metadata.get('date_published'),
    }

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

    # Getting key metadata from event
    event = json.loads(event['body']['body'])

    document_uid = event.get('uuid')
    url = event.get('uri')
    user_id = event.get('user_id')
    api_user = event.get('api_user')

    # Raise an error if there is no UUID in the document's S3 metadata
    assert document_uid, 'Document must have a UUID attached'

    # Extracting title and text from the HTML
    if "https://www.gov.uk/" in url:
        text, title, date_published = get_content(url)

    else:
        title, text = get_title_and_text(url)
        date_published = get_publication_modification_date(url)

    # Adding extracted metadata to event in order to attach it to ORPML header
    event['title'] = title
    event['date_published'] = date_published if date_published else None

    logger.info(f'Document title is: {title}'
                f'Publishing date is: {date_published}')

    # Build ORPML document (insert header and body)
    orpml_doc = process_orpml(text_body=text, metadata=event)

    # Write ORPML to S3
    s3_client = boto3.client('s3')
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
        'uri': url
    }
