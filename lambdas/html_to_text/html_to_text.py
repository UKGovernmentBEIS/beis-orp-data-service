import re
import os
from datetime import datetime
import boto3
import requests
import pandas as pd
from bs4 import BeautifulSoup
from htmldate import find_date
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


def get_title_and_text(URL):
    '''
    params: req: request URL
    returns: title, text: Str
    '''
    req = requests.get(URL)
    soup = BeautifulSoup(req.text, 'html.parser')

    title = str(soup.head.title.get_text())
    text = re.sub('\\s+', ' ', str(soup.get_text()).replace('\n', ' '))

    return title, text


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
    document_uid = event['body']['uuid']
    regulator_id = event['body']['regulator_id']
    user_id = event['body']['user_id']
    api_user = event['body'].get('api_user')
    document_type = event['body']['document_type']
    status = event['body']['status']
    url = event['body']['uri']
    regulatory_topic = event['body']['topics']

    title, text = get_title_and_text(url)
    date_published = get_publication_modification_date(url)

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
