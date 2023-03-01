import re
import os
import boto3
import string
import pymongo
import datetime
import pandas as pd
from http import HTTPStatus
from add_patterns import initialise_matcher
from dateutil.relativedelta import relativedelta
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()


SOURCE_BUCKET = os.environ['SOURCE_BUCKET']
DDB_USER = os.environ['DDB_USER']
DDB_PASSWORD = os.environ['DDB_PASSWORD']
DDB_DOMAIN = os.environ['DDB_DOMAIN']

ddb_connection_uri = f'mongodb://{DDB_USER}:{DDB_PASSWORD}@{DDB_DOMAIN}:27017/?directConnection=true'

# Initalise the matcher
nlp, matcher = initialise_matcher()


def download_text(s3_client, document_uid, bucket=SOURCE_BUCKET):
    '''Downloads the raw text from S3 ready for keyword extraction'''

    text = s3_client.get_object(
        Bucket=bucket,
        Key=f'processed/{document_uid}.txt'

    )['Body'].read().decode('utf-8')

    logger.info('Downloaded text')

    return text


def mongo_connect_and_push(document_uid,
                           date,
                           database=ddb_connection_uri,
                           tlsCAFile='./rds-combined-ca-bundle.pem'):
    '''Connects to the DocumentDB, finds the document matching our UUID and adds the date to it'''

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
                                   '$set': {'date_published': date}})
    db_client.close()

    logger.info('Sent to DocumentDB')

    return {'statusCode': HTTPStatus.OK}


def preprocess_text(text):
    """
    param: text: str
    returns: clean_text: text more easily read by matcher
    """
    txt = text.lower()
    # Add spaces between digits and str characters
    txt = re.sub(r"(?i)(?<=\d)(?=[a-z])|(?<=[a-z])(?=\d)", " ", txt)
    clean_words = ''.join(' / ' if c in string.punctuation else c for c in txt)
    clean_text = re.sub(r"\s+", ' ', clean_words)
    return clean_text


def clean_date(candidate_dates):
    """
    param: candidate_dates: List of dates found from text
    returns: date_list: cleaned List of dates found from text
    """
    if len(candidate_dates) == 0:
        return None
    else:
        date_list = []
        for date in candidate_dates:
            # If month isalph()
            if re.search('[a-zA-Z]', date):
                date_list.append(pd.to_datetime(date).isoformat())
            # Else if date is numeric
            elif len(date.split(" / ")[-1]) < 4:
                try:
                    date = "/".join([date.split(" / ")[0], "01", date.split(" / ")[1]])
                    date_list.append(pd.to_datetime(date).isoformat())
                except:
                    continue
            else:
                continue

        return date_list


def find_date(clean_text):
    """
    param: clean_text: text from preprocess_text function
    returns: date_list: list of dates found from text
    """
    doc = nlp(clean_text)
    matches = matcher(doc)

    candidate_dates = []
    for match_id, start, end in matches:
        # string_id = nlp.vocab.strings[match_id]  # Get string representation
        span = doc[start:end]  # The matched span
        candidate_dates.append(str(span).title())

    date_list = clean_date(candidate_dates)
    return date_list


def check_metadata_date_in_doc(metadata_date, date_list):
    """
    param: metadata_date: date pulled from document's metadata
    param: date_list: list of dates from the cleaned text
    returns: date / metadata_date: either date from text or metadata date
        If any date extracted from the text is within 3 months of the metadata date, return this date
    """
    if date_list == None:
        return metadata_date

    else:
        margin = relativedelta(months=3)

        datetime_obj = datetime.datetime.fromisoformat(metadata_date).date()
        upper_date = datetime_obj + margin
        lower_date = datetime_obj - margin

        # Find the closest date
        closest_date = min([datetime.datetime.fromisoformat(date).date() for date in date_list], key=lambda x: abs(x - datetime_obj))

        if upper_date >= closest_date >= lower_date:
            return pd.to_datetime(closest_date).isoformat()
        else:
            return metadata_date


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    # Get document id
    document_uid = event['document_uid']

    # Asserting that there is a published date
    assert event.get('date_published'), 'Document must have a publishing date'
    metadata_date = event['date_published']

    # Download raw text
    s3_client = boto3.client('s3')
    text = download_text(s3_client=s3_client, document_uid=document_uid)
    clean_text = preprocess_text(text=text)

    # Extract date from text
    date_list = find_date(clean_text=clean_text)

    # Check if metadata date appears near dates found
    date = check_metadata_date_in_doc(metadata_date=metadata_date, date_list=date_list)

    # Show date
    logger.info(f"Date published: {date}")

    response = mongo_connect_and_push(document_uid=document_uid, date=date)
    response['document_uid'] = document_uid

    return response

