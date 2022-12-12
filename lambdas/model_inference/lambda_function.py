import boto3
import io


def handler(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]
    object_size = event["Records"][0]["s3"]["object"]["size"]

    s3_client = boto3.client('s3')

    doc_stream = s3_client.get_object(
        Bucket=bucket_name,
        Key=object_key
    )['Body']

    metadata = s3_client.head_object(
        Bucket=bucket_name,
        Key=object_key
    )['Metadata']

    doc_bytes = doc_stream.read()
    doc_bytes_io = io.BytesIO(doc_bytes)

    uuid = metadata['uuid']

    print(f"New document in {bucket_name}: {object_key}, with size: {object_size}")
    print(f"Document text: {doc_bytes_io}")
    print(f"UUID obtained is: {uuid}")

    # Create a MongoDB client, open a connection to Amazon DocumentDB as a
    # replica set and specify the read preference as secondary preferred

    # os.system('wget https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem')

    # db_client = pymongo.MongoClient(
    #     ("mongodb://ddbadmin:Test123456789@beis-orp-dev-beis-orp.cluster-cau6o2mf7iuc."
    #      "eu-west-2.docdb.amazonaws.com:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&"
    #      "replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"))
    # db = db_client.documents
    # col = db.testing

    # col.insert_one(
    #     {
    #         "title": title,
    #         "uuid": uuid
    #     }
    # )

    # db_client.close()

    return {
        'statusCode': 200
    }
