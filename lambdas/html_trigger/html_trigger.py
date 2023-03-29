# import os
import json
import boto3
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()

# STATE_MACHINE_ARN = os.environ['STATE_MACHINE']
STATE_MACHINE = 'arn:aws:states:eu-west-2:412071276468:stateMachine:orp_document_ingestion'


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    # Get the JSON payload from the POST request
    payload = event['payload']
    logger.info(f'Received event with a payload: {payload}')

    # Create a Step Functions client
    sf_client = boto3.client('stepfunctions')

    # Define the ARN of the Step Functions state machine to trigger
    state_machine_arn = STATE_MACHINE

    # Define the input for the state machine execution as the payload received
    # in the POST request
    input = json.dumps(payload)

    # Trigger the state machine execution
    logger.info('Triggering Step Functions')
    sf_response = sf_client.start_execution(
        stateMachineArn=state_machine_arn,
        input=input
    )

    handler_response = sf_response['ResponseMetadata']

    return handler_response
