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


def merge_dicts(dict_list):
    '''Merge all (changed) inputs into one dictionary'''

    merged_dict = {}
    for dictionary in dict_list:
        if dictionary['lambda'] == 'date_generation':
            merged_dict.setdefault('data', {}).setdefault('dates', {})[
                'date_published'] = dictionary['document']['data']['dates']['date_published']
            merged_dict['data']['dates']['date_uploaded'] = dictionary['document']['data']['dates']['date_uploaded']
        elif dictionary['lambda'] == 'keyword_extraction':
            merged_dict['subject_keywords'] = dictionary['document']['subject_keywords']
            # Title is added here as the keyword_extraction lambda runs immediately
            # after the title_generation lambda
            merged_dict['title'] = dictionary['document']['title']
        elif dictionary['lambda'] == 'summarisation':
            merged_dict['summary'] = dictionary['document']['summary']
            merged_dict['language'] = dictionary['document']['language']
        elif dictionary['lambda'] == 'legislative_origin_extraction':
            merged_dict.setdefault(
                'data', {})['legislative_origins'] = dictionary['document']['data']['legislative_origins']
        else:
            raise UserWarning(
                f'Unexpected lambda input received: {dictionary["lambda"]}')

    return merged_dict


def assert_same_base_values(keys, dict_list):
    '''
    Raises an assertion error if the inputs received from the parallel stage
    don't have the same base values
    '''
    # Get the values of the specified keys in each dictionary
    values = set()
    for d in dict_list:
        values.add(
            tuple(
                tuple(v) if isinstance(
                    v, list) else v for v in (
                    d['document'][k] for k in keys)))

    # Check if all values are the same
    assert len(values) == 1, 'The base values of the inputs received are not the same'

    base_document = {k: v for k,
                     v in dict_list[0]['document'].items() if k in keys}
    return base_document


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

    # Ensuring the base outputs of the parallel stage are the same
    base_keys = [
        'document_uid',
        'regulator_id',
        'regulatory_topic',
        'user_id',
        'uri',
        'document_type',
        'document_format',
        'status',
        'hash_text']
    base_document = assert_same_base_values(keys=base_keys, dict_list=event)

    # Each previous lambda has added a new key to the extracted metadata
    # so we need to merge the metadata docs
    inferred_document = merge_dicts(dict_list=event)
    document = {**base_document, **inferred_document}
    logger.info({'document': document})

    ############ TODO UNCOMMENT LINE BELOW  
    # response = sqs_connect_and_send(document=document)

    # Obtaining values for user_id and whether or not the user has uploaded
    # via GUI or API
    user_id = document['user_id']
    api_user = event[0]['api_user']

    # Send an email to the user if ingestion is successful and if the user is
    # not an API user

    ############ TODO UNCOMMENT LINE BELOW  
    # if response['ResponseMetadata']['HTTPStatusCode'] == 200 and not api_user:

    ############ TODO RE-INDENT CODE BLOCK BELOW
    email_address = get_email_address(COGNITO_USER_POOL, user_id)
    logger.info(f'Pulled email from Cognito: {email_address}')

    if email_address:
        document_uid = document['document_uid']
        title = document['title']
        document_type = document['document_type']
        regulator_id = document['regulator_id']
        date_published = document['data']['dates']['date_published']

        send_email(
            sender_email=SENDER_EMAIL_ADDRESS,
            recipient_email=email_address,
            subject='ORP Upload Complete',
            body=f'''Your document (UUID: {document_uid}) has been ingested to the ORP.
                It can be viewed in the ORP at
                https://app.{ENVIRONMENT}.cannonband.com/document/view/{document_uid}?ingested=true
                It will now be searchable.\n
                You can search using the following criteria:\n
                - Title: {title}\n
                - Document Type: {document_type}\n
                - Regulator: {regulator_id}\n
                - Date Published: {date_published}\n
                This is a system generated email, please do not reply.'''
        )

    ############ TODO DELETE LINE BELOW  
    response = {}

    return response
