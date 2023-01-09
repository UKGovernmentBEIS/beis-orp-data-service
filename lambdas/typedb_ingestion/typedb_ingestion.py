import boto3
import pymongo
import json


SOURCE_DATABASE = ("mongodb://ddbadmin:Test123456789@beis-orp-dev-beis-orp.cluster-cau6o2mf7iuc."
                   "eu-west-2.docdb.amazonaws.com:27017/?directConnection=true")
DESTINATION_QUEUE_URL = "https://sqs.eu-west-2.amazonaws.com/455762151948/update-typedb"


def handler(event, context):
    print(f"Event received: {event}")
    document_uid = event["responsePayload"]["document_uid"]

    # Create a MongoDB client and open a connection to Amazon DocumentDB
    db_client = pymongo.MongoClient(
        SOURCE_DATABASE,
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
        QueueUrl=DESTINATION_QUEUE_URL,
        MessageBody=json.dumps(document)
    )

    print(response)
    return response
