import os
import json
import boto3
from datetime import datetime
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

STATE_MACHINE_ARN = os.environ['STATE_MACHINE_ARN']


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    # Get the JSON payload from the POST request
    payload = event
    logger.info(f'Received event with a payload: {payload}')

    payload['time'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    # Create a Step Functions client
    sf_client = boto3.client('stepfunctions')

    # Define the ARN of the Step Functions state machine to trigger
    state_machine_arn = STATE_MACHINE_ARN

    # Define the input for the state machine execution as the payload received
    # in the POST request
    input = {
        'body': payload,
        'detail': {
            'object': {
                'key': 'HTML'
            }
        }
    }
    input_str = json.dumps(input)

    # Trigger the state machine execution
    logger.info('Triggering Step Functions')
    sf_response = sf_client.start_execution(
        stateMachineArn=state_machine_arn,
        input=input_str
    )

    handler_response = sf_response['ResponseMetadata']

    return handler_response
