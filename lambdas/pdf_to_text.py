# import json


def handler(event, context):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_key = event["Records"][0]["s3"]["object"]["key"]
    object_size = event["Records"][0]["s3"]["object"]["size"]
    print(f"New document in {bucket_name}: {object_key}, with size: {object_size}")
    # TODO implement
    return {
        'statusCode': 200,
        'body': 'Hello from Lambda!'
    }
