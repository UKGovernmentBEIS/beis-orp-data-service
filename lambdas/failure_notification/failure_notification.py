import os
import json
import boto3
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

COGNITO_USER_POOL = os.environ['COGNITO_USER_POOL']
SENDER_EMAIL_ADDRESS = os.environ['SENDER_EMAIL_ADDRESS']
ENVIRONMENT = os.environ['ENVIRONMENT']


def get_email_address(user_pool_id, user_sub):
    logger.info(f'Retrieving email from Cognito for user: {user_sub}')
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

    if event['detail']['object']['key'] == 'HTML':
        document = json.loads(event['body']['body'])
        failed_doc = document['uri']
        document_uid = document.get('document_uid', document.get('uuid'))
        uploader_id = document['user_id']
    else:
        failed_doc = event['document']['object_key']
        uploader_id = failed_doc['user_id']

    error = event.get('error').get('Error')
    cause = event.get('error').get('Cause')

    uploader_email = get_email_address(
        user_pool_id=COGNITO_USER_POOL,
        user_sub=uploader_id)
    logger.info(f'Retrieved email from Cognito: {uploader_email}')

    if uploader_email:
        logger.error(f'''The ORP failed to ingest a document.
                    Uploader: {uploader_email}
                    Document S3 key or URL: {failed_doc}
                    Error: {error}
                    Cause: {cause}''')

        if document_uid:

            body = f'''Your document (UUID: {document_uid}) has not been uploaded to the ORP.
                        It can be viewed in the ORP at
                        https://app.{ENVIRONMENT}.open-regulation.beis.gov.uk/document/view/{document_uid}?ingested=true
                        However it will not be searchable as uploading the document caused a {error}.
                        If you know the cause of this error, please fix it and re-upload the document.
                        If not, reach out to {SENDER_EMAIL_ADDRESS} and they will look into it further.
                        Thank you for using the ORP.
                        This is a system generated email, please do not reply.'''

        else:

            body = f'''Your document (UUID: {document_uid}) has not been uploaded to the ORP.
                        Uploading the document caused an error: {error}.
                        If you the know cause of this error, please fix it and re-upload the document.
                        If not, reach out to {SENDER_EMAIL_ADDRESS} and they will look into it further.
                        Thank you for using the ORP.
                        This is a system generated email, please do not reply.'''

        logger.info(f'Sending email from: {SENDER_EMAIL_ADDRESS} to: {uploader_email}')
        response = send_email(
            sender_email=SENDER_EMAIL_ADDRESS,
            recipient_email=uploader_email,
            subject='ORP Upload Failure',
            body=body
        )

        logger.info('Sent email')

        return {
            'Uploader': uploader_email,
            'Failed Doc': failed_doc,
            'Error': error,
            'Cause': cause,
            'Email Send HTTP Response': response
        }

    else:

        return {'Notification Failure': 'Couldn\'t find uploader in Cognito'}
