import re
import os
import boto3
import pymongo
import requests
from http import HTTPStatus
from bs4 import BeautifulSoup
from htmldate import find_date
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DDB_USER = os.environ['DDB_USER']
DDB_PASSWORD = os.environ['DDB_PASSWORD']
DDB_DOMAIN = os.environ['DDB_DOMAIN']
DESTINATION_BUCKET = os.environ['DESTINATION_BUCKET']


ddb_connection_uri = f'mongodb://{DDB_USER}:{DDB_PASSWORD}@{DDB_DOMAIN}:27017/?directConnection=true'


def get_title_text(URL):
    """
    params: req: request URL
    returns: title, text: Str
    """
    req = requests.get(URL)
    soup = BeautifulSoup(req.text, "html.parser")

    title = str(soup.head.title.get_text())
    text = re.sub("\\s+", " ", str(soup.get_text()).replace("\n", " "))

    return title, text


def get_publication_modification_date(URL):
    """
    params: URL: Str
    returns: publication_date, modification_date: Str
    """
    # Initally disable extensive search
    publication_date = str(find_date(URL, original_date=True, extensive_search=False))
    modification_date = str(find_date(URL, extensive_search=False))

    # If no concrete date is found, do extensive search
    if publication_date == "None":
        publication_date = "Inferred " + str(find_date(URL, original_date=True))

    if modification_date == "None":
        modification_date = "Inferred " + str(find_date(URL))

    return publication_date, modification_date


def mongo_connect_and_push(document_uid,
                           title,
                           date_published,
                           database=ddb_connection_uri,
                           tlsCAFile='./rds-combined-ca-bundle.pem'):
    '''Connects to the DocumentDB, finds the document matching our UUID and adds the title, text and publishing date to it'''

    db_client = pymongo.MongoClient(
        database,
        tls=True,
        tlsCAFile=tlsCAFile
    )
    logger.info(db_client.list_database_names())
    db = db_client.bre_orp
    collection = db.documents

    # Insert document to DB
    logger.info({'document': collection.find_one(
        {'document_uid': document_uid})})
    collection.find_one_and_update({'document_uid': document_uid}, {
                                   '$set': {'title': title},
                                   '$set': {'date_published': date_published}})
    db_client.close()

    logger.info('Sent to DocumentDB')

    return {'statusCode': HTTPStatus.OK}


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

    return 


def handler(event, context: LambdaContext):

    s3_client = boto3.client('s3')

    logger.set_correlation_id(context.aws_request_id)

    # Get document id
    document_uid = event['document_uid']
    # Download raw pdf and extracted text
    URL = event['detail']['url']
    # Get metadata title
    title, text = get_title_text(URL)
    # Get publishing date
    date_published = get_publication_modification_date(URL)

    logger.info(f"Document title is: {title} "
                f"Publishing date is: {date_published}")

    mongo_response = mongo_connect_and_push(document_uid, title, text, date_published)
    s3_response = write_text(s3_client, text=text, document_uid=document_uid)

    handler_response = {**mongo_response, **s3_response}
    handler_response['document_uid'] = document_uid

    return handler_response

