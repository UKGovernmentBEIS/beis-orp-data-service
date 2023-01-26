import boto3
import pymongo
import json
import os


DOCUMENT_DATABASE = os.environ["DOCUMENT_DATABASE"]
DESTINATION_SQS_URL = os.environ["DESTINATION_SQS_URL"]


def handler(event, context):
    print(f"Event received: {event}")
    document_uid = event["document_uid"]

    # Create a MongoDB client and open a connection to Amazon DocumentDB
    db_client = pymongo.MongoClient(
        DOCUMENT_DATABASE,
        tls=True,
        tlsCAFile="./rds-combined-ca-bundle.pem"
    )
    print("Connected to DocumentDB")

    db = db_client.bre_orp
    collection = db.documents
    query = {
        "document_uid": document_uid
    }

    # Find document matching the UUID
    document = collection.find_one(query)
    del document['_id']

    # Print the query result to the screen
    print(f"Document found: {document}")
    db_client.close()

    # Create an SQS client and send the document
    sqs = boto3.client("sqs")
    print("Sending document to SQS")
    response = sqs.send_message(
        QueueUrl=DESTINATION_SQS_URL,
        MessageBody=json.dumps(document)
    )

    print(response)
    return response
