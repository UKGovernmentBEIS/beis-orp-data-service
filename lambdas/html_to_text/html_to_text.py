import re
import os
import json
from datetime import datetime
import boto3
import requests
import pandas as pd
from notification_email import send_email
from bs4 import BeautifulSoup
from htmldate import find_date
from govuk_extraction import get_content
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']
COGNITO_USER_POOL = os.environ['COGNITO_USER_POOL']
SENDER_EMAIL_ADDRESS = os.environ['SENDER_EMAIL_ADDRESS']


def get_title_and_text(URL):
    '''
    params: req: request URL
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


def get_publication_modification_date(URL):
    '''
    params: URL: Str
    returns: publication_date: Str
    '''
    # Initally disable extensive search
    publication_date = str(
        find_date(URL, original_date=True, extensive_search=False))
    modification_date = str(find_date(URL, extensive_search=False))

    # If no concrete date is found, do extensive search
    if (publication_date == 'None') and (modification_date == 'None'):
        publication_date = str(find_date(URL, original_date=True))

    publication_date = pd.to_datetime(publication_date).isoformat()

    return publication_date


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

    # Finding the time the object was uploaded
    date_uploaded = datetime.now()
    date_uploaded_formatted = date_uploaded.strftime('%Y-%m-%dT%H:%M:%S')

    s3_client = boto3.client('s3')

    # Getting metadata from event
    event = json.loads(event['body']['body'])
    document_uid = event['uuid']
    regulator_id = event['regulator_id']
    user_id = event['user_id']
    api_user = event.get('api_user')
    document_type = event['document_type']
    status = event['status']
    url = event['uri']
    regulatory_topic = event['topics']

    if "https://www.gov.uk/" in url:
        text, title, date_published = get_content(url)

    else:
        response = get_title_and_text(url)
        date_published = get_publication_modification_date(url)

        # If response is None, notify the uploader of a bad url
        if response is None:
            logger.info("Bad URL uploaded")
            send_email(
                COGNITO_USER_POOL,
                SENDER_EMAIL_ADDRESS,
                user_id=user_id,
                url=url
            )
            # TODO CHANGE RETURN EMPTY DICTIONARY
            handler_response = {}
            return handler_response

        else:
            title, text = response

    logger.info(f'Document title is: {title}'
                f'Publishing date is: {date_published}')

    write_text(s3_client, text=text, document_uid=document_uid)

    logger.info(f'All data extracted e.g. Title extracted: {title}')

    # Building metadata document
    doc = {
        'title': title,
        'document_uid': document_uid,
        'regulator_id': regulator_id,
        'user_id': user_id,
        'uri': url,
        'data':
        {
            'dates':
            {
                'date_published': date_published,
                'date_uploaded': date_uploaded_formatted
            }
        },
        'document_type': document_type,
        'document_format': 'HTML',
        'regulatory_topic': regulatory_topic,
        'status': status,
    }

    handler_response = {
        'document': doc,
        'api_user': api_user
    }

    return handler_response
