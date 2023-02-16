import re
import os
import boto3
import string
import pymongo
import datetime
import datefinder
from http import HTTPStatus
from add_patterns import initialise_matcher
from dateutil.relativedelta import relativedelta
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()


DOCUMENT_DATABASE = os.environ['DOCUMENT_DATABASE']
SOURCE_BUCKET = os.environ['SOURCE_BUCKET']


# Initalise the matcher
nlp, matcher = initialise_matcher()


def addSpace(text):
    return re.sub(r'(?<=([a-z])|\d)(?=(?(1)\d|[a-z]))', ' ', text)


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
                           database=DOCUMENT_DATABASE,
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
    txt = text.lower()
    # Add spaces between digits and str characters
    txt = re.sub(r"(?i)(?<=\d)(?=[a-z])|(?<=[a-z])(?=\d)", " ", txt)
    clean_words = ''.join(' / ' if c in string.punctuation else c for c in txt)
    clean_text = re.sub(r"\s+", ' ', clean_words) 
    return clean_text


def standardise_date(date):
    matches = datefinder.find_dates(date)
    date_matches = [str(date) for date in matches]
    return date_matches


def clean_date(candidate_dates):
    if len(candidate_dates) == 0:
        return None
    else:
        date_list = []
        for date in candidate_dates:
            if re.search('[a-zA-Z]', date):
                date_list.append(standardise_date(date))
            elif len(date.split(" / ")[-1]) < 4:
                if date.split(" / ")[-1][0] == "9" or date.split(" / ")[-1][0] == "8" or date.split(" / ")[-1][0] == "7":
                    date = "".join(date.split(" / ")[:-1]) + " / " + "19" + "".join(date.split(" / ")[-1])
                    date_list.append(standardise_date(date))
                else: 
                    date = "".join(date.split(" / ")[:-1]) + " / " + "20" + "".join(date.split(" / ")[-1])
                    date_list.append(standardise_date(date))
        return date_list


def find_date(clean_text):
    doc = nlp(clean_text)
    matches = matcher(doc)

    candidate_dates = []
    for match_id, start, end in matches:
        string_id = nlp.vocab.strings[match_id]  # Get string representation
        span = doc[start:end]  # The matched span
        candidate_dates.append(str(span).title())

    date_list = clean_date(candidate_dates)
    return date_list


def check_metadata_date_in_doc(metadata_date, date_list):
    margin = relativedelta(months = 3)

    datetime_obj = datetime.datetime.strptime(metadata_date[0], '%Y-%m-%d %H:%M:%S')
    upper_date = datetime_obj + margin
    lower_date = datetime_obj - margin

    for date in date_list:
        date =  datetime.datetime.strptime(date[0], '%Y-%m-%d %H:%M:%S')
        if  upper_date > date > lower_date:
            return date
        else: 
            return metadata_date


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    # Get document id
    document_uid = event['document_uid']
    metadata_date = event['date_published']

    # Download raw text
    s3_client = boto3.client('s3')
    text = download_text(s3_client, document_uid)
    clean_text = preprocess_text(text)

    # Extract date from text
    date_list = find_date(clean_text=clean_text)

    # Check if metadata date appears near dates found
    date = check_metadata_date_in_doc(metadata_date=metadata_date, date_list=date_list)

    response = mongo_connect_and_push(document_uid, date)
    response['document_uid'] = document_uid

    return response


