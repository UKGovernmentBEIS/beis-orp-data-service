import json
import os
import boto3
import pymongo
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DDB_USER = os.environ['DDB_USER']
DDB_PASSWORD = os.environ['DDB_PASSWORD']
DDB_DOMAIN = os.environ['DDB_DOMAIN']
DESTINATION_SQS_URL = os.environ['DESTINATION_SQS_URL']

ddb_connection_uri = f'mongodb://{DDB_USER}:{DDB_PASSWORD}@{DDB_DOMAIN}:27017/?directConnection=true'


def mongo_connect_and_pull(document_uid,
                           database,
                           tlsCAFile='./rds-combined-ca-bundle.pem'):
    '''Connects to the DocumentDB, finds the document matching our UUID and pulls it'''

    db_client = pymongo.MongoClient(
        database,
        tls=True,
        tlsCAFile=tlsCAFile
    )
    logger.info('Succesfully Connected')
    db = db_client.bre_orp
    collection = db.documents

    query = {'document_uid': document_uid}
    document = collection.find_one(query)
    del document['_id']
    db_client.close()

    return document


def sqs_connect_and_send(document, queue=DESTINATION_SQS_URL):
    '''Create an SQS client and send the document'''

    sqs = boto3.client('sqs')
    response = sqs.send_message(
        QueueUrl=queue,
        MessageBody=json.dumps(document)
    )

    return response


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    document_uid = event['document_uid']
    logger.append_keys(document_uid=document_uid)

    document = mongo_connect_and_pull(
        document_uid=document_uid,
        database=ddb_connection_uri)
    logger.info({'document': document})
    response = sqs_connect_and_send(document=document)
    logger.info({'sqs_response': response})

    return response
