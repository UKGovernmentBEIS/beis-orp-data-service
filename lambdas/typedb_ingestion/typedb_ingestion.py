import json
import os
import boto3
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

DESTINATION_SQS_URL = os.environ['DESTINATION_SQS_URL']


def merge_dicts(*args):
    merged_dict = {}

    # Merge all dictionaries into the new dictionary
    for dictionary in args:
        for key, value in dictionary.items():
            if value is None:
                continue
            if key not in merged_dict:
                merged_dict[key] = value
            elif merged_dict[key] == value:
                continue
            elif isinstance(merged_dict[key], list):
                if value not in merged_dict[key]:
                    merged_dict[key].append(value)
            else:
                merged_dict[key] = [merged_dict[key], value]

    return merged_dict


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

    # Ensuring the outputs of the parallel stage are the same
    # assert all(map(lambda x: x == event[0], event)
    #            ), f'Outputs of parallel stage are not the same: {event}'

    # Each previous lambda has added a new key to the extracted metadata
    # so we need to merge the metadata docs
    lambda_responses = [response['document'] for response in event]
    document = merge_dicts(lambda_responses)

    logger.info({'document': document})
    response = sqs_connect_and_send(document=document)
    logger.info({'sqs_response': response})

    return response
