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

        LOGGER.info(f'Email sent from to {recipient_email}')
        return response

    except Exception as e:
        LOGGER.error(f'Email failed to send. Error message: {e}')