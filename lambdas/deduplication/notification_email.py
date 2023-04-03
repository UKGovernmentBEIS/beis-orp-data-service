import os
import boto3
import logging

LOGGER = logging.getLogger()
LOGGER.setLevel(int(os.environ.get("LOGGING_LEVEL", logging.INFO)))


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

        LOGGER.info(f'Email sent from to {recipient_email}')
        return response

    except Exception as e:
        LOGGER.error(f'Email failed to send. Error message: {e}')


def send_email(COGNITO_USER_POOL, SENDER_EMAIL_ADDRESS,
               user_id, complete_existing_metadata):
               
    email_address = get_email_address(user_pool_id=COGNITO_USER_POOL,
                                             user_sub=user_id)
    LOGGER.info(f'Pulled email from Cognito: {email_address}')

    if email_address:
        user_id = complete_existing_metadata['user_id']
        document_uid = complete_existing_metadata['document_uid']

        send_email_structure(
            sender_email=SENDER_EMAIL_ADDRESS,
            recipient_email=email_address,
            subject='ORP Upload Rejected',
            body=f'''Your document (UUID: {document_uid}) has been flagged as a duplicate.
                The existing document can be viewed in the ORP at https://app.{ENVIRONMENT}.cannonband.com/document/view/{document_uid}
                This is a system generated email, please do not reply.'''
        )
