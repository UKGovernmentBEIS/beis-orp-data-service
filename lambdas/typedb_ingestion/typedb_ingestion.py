import json
import os
import boto3
from operator import itemgetter
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_SQS_URL = os.environ['DESTINATION_SQS_URL']


def merge_dicts(dict_list):
    '''Merge all (changed) inputs into one dictionary'''

    merged_dict = {}
    for dictionary in dict_list:
        if dictionary['lambda'] == 'date_generation':
            merged_dict['data']['dates']['date_published'] = dictionary['document']['data']['dates']['date_published']
        elif dictionary['lambda'] == 'title_generation':
            merged_dict['title'] = dictionary['document']['title']
        elif dictionary['lambda'] == 'keyword_extraction':
            merged_dict['subject_keywords'] = dictionary['document']['subject_keywords']
        elif dictionary['lambda'] == 'summarisation':
            merged_dict['summary'] = dictionary['document']['summary']
        else:
            raise UserWarning('Unexpected lambda input received')

    return merged_dict


def assert_same_base_values(keys, dict_list):
    '''
    Raises an assertion error if the inputs received from the parallel stage 
    don't have the same base values
    '''
    # Get the values of the specified keys in each dictionary
    values = set(itemgetter(*keys)(d['document']) for d in dict_list)
    # Check if all values are the same
    assert len(
        values) == 1, f'The base values of the inputs received are not the same'

    return itemgetter(*keys)(dict_list[0])


def sqs_connect_and_send(document, queue=DESTINATION_SQS_URL):
    '''Create an SQS client and send the document'''

    sqs = boto3.client('sqs')
    response = sqs.send_message(
        QueueUrl=queue,
        MessageBody=json.dumps(document, indent=4, sort_keys=True, default=str)
    )

    return response


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    # Ensuring the base outputs of the parallel stage are the same
    base_keys = ['document_uid', 'regulator_id', 'user_id',
                 'uri', 'document_type', 'status']
    base_document = assert_same_base_values(keys=base_keys, dict_list=event)

    # Each previous lambda has added a new key to the extracted metadata
    # so we need to merge the metadata docs
    inferred_document = merge_dicts(dict_list=event)
    document = {**base_document, **inferred_document}
    logger.info({'document': document})

    response = sqs_connect_and_send(document=document)
    logger.info({'sqs_response': response})

    return response
