# import os
# import json
# import boto3
from datetime import datetime
from aws_lambda_powertools.logging.logger import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext


logger = Logger()


@logger.inject_lambda_context(log_event=True)
def handler(event, context: LambdaContext):
    logger.set_correlation_id(context.aws_request_id)

    # Get the JSON payload from the POST request
    payload = event

    logger.info(f'Received event with a payload: {payload}')

    payload['time'] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    return payload
