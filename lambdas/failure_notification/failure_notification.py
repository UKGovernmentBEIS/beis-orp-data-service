import os
import json
import boto3
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

COGNITO_USER_POOL = os.environ['COGNITO_USER_POOL']
SENDER_EMAIL_ADDRESS = os.environ['SENDER_EMAIL_ADDRESS']


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


def send_email_structure(sender_email, recipient_email, subject, body):
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


def send_email(COGNITO_USER_POOL, SENDER_EMAIL_ADDRESS,
               user_id, url):

    email_address = get_email_address(user_pool_id=COGNITO_USER_POOL, user_sub=user_id)
    logger.info(f'Pulled email from Cognito: {email_address}')

    if email_address:

        send_email_structure(
            sender_email=SENDER_EMAIL_ADDRESS,
            recipient_email=email_address,
            subject='ORP Upload Rejected',
            body=f'''We have been unable to process (URL: {url}).
                Please check the URL is not a redirecting URL.
                This is a system generated email, please do not reply.'''
        )


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    if event['detail']['object']['key'] == 'HTML':
        document = json.loads(event['body']['body'])
        failed_doc = document['uri']
        user = document['user_id']
    else:
        failed_doc = event['document']['object_key']
        user = failed_doc['user_id']

    error = event.get('error')

    logger.info(f'user: {user}')
    logger.info(f'failed_doc_key: {failed_doc}')
    logger.info(f'Error: {error}')

    return {
        'user': user,
        'failed_doc': failed_doc,
        'error': error
    }
