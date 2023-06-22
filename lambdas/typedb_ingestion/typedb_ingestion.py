import json
import os
import boto3
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_SQS_URL = os.environ['DESTINATION_SQS_URL']
COGNITO_USER_POOL = os.environ['COGNITO_USER_POOL']
SENDER_EMAIL_ADDRESS = os.environ['SENDER_EMAIL_ADDRESS']
ENVIRONMENT = os.environ['ENVIRONMENT']


def sqs_connect_and_send(document, queue=DESTINATION_SQS_URL):
    '''Create an SQS client and send the document'''

    sqs = boto3.client('sqs')
    response = sqs.send_message(
        QueueUrl=queue,
        MessageBody=json.dumps(document, indent=4, sort_keys=True, default=str)
    )

    return response


def get_email_address(user_pool_id, user_sub):
    cognito_client = boto3.client('cognito-idp')
    try:
        response = cognito_client.admin_get_user(
            UserPoolId=user_pool_id,
            Username=user_sub
        )
        for attribute in response['UserAttributes']:
            if attribute['Name'] == 'email':
                return attribute['Value']
    except cognito_client.exceptions.UserNotFoundException:
        return None


def send_email(sender_email, recipient_email, subject, body):
    ses_client = boto3.client('ses')
    try:
        response = ses_client.send_email(
            Source=sender_email,
            Destination={
                'ToAddresses': [
                    recipient_email,
                ],
            },
            Message={
                'Subject': {
                    'Data': subject,
                },
                'Body': {
                    'Text': {
                        'Data': body,
                    },
                },
            },
        )

        logger.info(f'Email sent from to {recipient_email}')
        return response

    except Exception as e:
        logger.error(f'Email failed to send. Error message: {e}')


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    document = event
    logger.info({'document': document})

    response = sqs_connect_and_send(document=document)

    # Obtaining values for user_id and whether or not the user has uploaded
    # via GUI or API
    user_id = document['user_id']

    # Send an email to the user if ingestion is successful and if the user is
    # not an API user
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:

        email_address = get_email_address(COGNITO_USER_POOL, user_id)
        logger.info(f'Pulled email from Cognito: {email_address}')

        if email_address:
            document_uid = document['document_uid']
            title = document['title']
            date_published = document['data']['dates']['date_published']

            send_email(
                sender_email=SENDER_EMAIL_ADDRESS,
                recipient_email=email_address,
                subject='ORP Upload Complete',
                body=f'''The ORP ingestion pipeline has finished enriching your document (UUID: {document_uid}).
                    The upload process has begun and should take no longer than 2 minutes.
                    Once uploaded, it can be viewed in the ORP at
                    https://app.{ENVIRONMENT}.open-regulation.beis.gov.uk/document/view/{document_uid}?ingested=true
                    and will be searchable.\n
                    If the document is not visible after 5 minutes, please contact {SENDER_EMAIL_ADDRESS} as there\n
                    may have been an issue during upload.
                    You can search using the following criteria:\n
                    - Title: {title}\n
                    - Date Published: {date_published}\n
                    This is a system generated email, please do not reply.'''
            )

    return response
